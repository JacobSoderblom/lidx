//! What the Go import hook needs to know about the repository: its Go
//! modules (every `go.mod`, nested ones and `replace` directives that point
//! at in-repo directories included), vendored packages, and the name each
//! package directory declares. Built lazily once per index run (see
//! `GoExtractor::begin_run`), so an incremental sync sees what a fresh
//! reindex would.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;

/// One `go.mod` of the repository.
struct GoModule {
    /// The `module` path.
    path: String,
    /// The directory of the `go.mod`, relative to the repo root (`""` = root).
    dir: String,
}

/// Where an import path points.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ImportTarget {
    /// A package directory of this repository (relative to the repo root).
    InRepo(String),
    /// Anything else: the standard library or a third-party module.
    Foreign,
}

pub(crate) struct GoRepo {
    modules: Vec<GoModule>,
    /// `replace <import prefix> => <in-repo dir>`.
    replaces: Vec<(String, String)>,
    /// Package directory -> the name its `package` clause declares.
    package_names: RefCell<HashMap<String, Option<String>>>,
}

impl GoRepo {
    pub(crate) fn scan(repo_root: &Path) -> Self {
        let mut modules = Vec::new();
        let mut replaces = Vec::new();
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
            // The go tool never builds `testdata`: a module there is a sample
            // (syft has several that declare the root module's own path).
            if dir.split('/').any(|part| part == "testdata") {
                continue;
            }
            let Some(path) = module_path(&text) else {
                continue;
            };
            for (prefix, target) in replace_directives(&text) {
                if let Some(rel) = relative_target(&dir, &target) {
                    replaces.push((prefix, rel));
                }
            }
            modules.push(GoModule { path, dir });
        }
        // Deterministic whatever order the walk produced.
        modules.sort_by(|a, b| a.dir.cmp(&b.dir));
        replaces.sort();
        Self {
            modules,
            replaces,
            package_names: RefCell::new(HashMap::new()),
        }
    }

    /// Whether the repo has any `go.mod`: without one nothing is known about
    /// what an import path names.
    pub(crate) fn has_modules(&self) -> bool {
        !self.modules.is_empty()
    }

    /// The directory of the nearest `go.mod` at or above `file_rel_path`.
    fn module_dir_of(&self, file_rel_path: &str) -> Option<&str> {
        self.modules
            .iter()
            .filter(|m| {
                m.dir.is_empty()
                    || file_rel_path
                        .strip_prefix(m.dir.as_str())
                        .is_some_and(|rest| rest.starts_with('/'))
            })
            .max_by_key(|m| m.dir.len())
            .map(|m| m.dir.as_str())
    }

    /// Where `import_path`, imported from `file_rel_path`, points. The
    /// longest matching in-repo module path or `replace` prefix wins (a
    /// module's own path is its root package); failing that a vendored copy
    /// under the importing module's `vendor/`.
    pub(crate) fn resolve_import(
        &self,
        repo_root: &Path,
        file_rel_path: &str,
        import_path: &str,
    ) -> ImportTarget {
        let roots = self
            .modules
            .iter()
            .map(|m| (m.path.as_str(), m.dir.as_str()))
            .chain(self.replaces.iter().map(|(p, d)| (p.as_str(), d.as_str())));
        let best = roots
            .filter_map(|(prefix, dir)| {
                let rest = if import_path == prefix {
                    ""
                } else {
                    import_path.strip_prefix(prefix)?.strip_prefix('/')?
                };
                let depth = dir.split('/').filter(|p| !p.is_empty()).count();
                Some((prefix.len(), std::cmp::Reverse(depth), join(dir, rest)))
            })
            // The longest prefix wins; of equal ones the shallowest directory
            // (the main module over a nested module repeating its path).
            .max_by_key(|(len, depth, _)| (*len, *depth));
        if let Some((_, _, dir)) = best {
            return ImportTarget::InRepo(dir);
        }
        if let Some(module_dir) = self.module_dir_of(file_rel_path) {
            let vendored = join(&join(module_dir, "vendor"), import_path);
            if repo_root.join(&vendored).is_dir() {
                return ImportTarget::InRepo(vendored);
            }
        }
        ImportTarget::Foreign
    }

    /// The package name declared in directory `dir` (relative to the repo
    /// root): the `package` clause of its first non-test `.go` file, else of
    /// its first test file (minus a `_test` suffix).
    pub(crate) fn package_name(&self, repo_root: &Path, dir: &str) -> Option<String> {
        if let Some(known) = self.package_names.borrow().get(dir) {
            return known.clone();
        }
        let name = read_package_name(&repo_root.join(dir));
        self.package_names
            .borrow_mut()
            .insert(dir.to_string(), name.clone());
        name
    }
}

fn join(dir: &str, rest: &str) -> String {
    match (dir, rest) {
        ("", rest) => rest.to_string(),
        (dir, "") => dir.to_string(),
        (dir, rest) => format!("{dir}/{rest}"),
    }
}

fn module_path(go_mod: &str) -> Option<String> {
    go_mod.lines().find_map(|line| {
        let rest = line.split("//").next()?.trim().strip_prefix("module")?;
        let path = rest.trim().trim_matches('"');
        (!path.is_empty() && rest.starts_with(char::is_whitespace)).then(|| path.to_string())
    })
}

/// `(import prefix, replacement)` of every `replace` directive, block form
/// included. A versioned left side (`a/b v1.2.3 => ..`) keeps only the path.
fn replace_directives(go_mod: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut in_block = false;
    for line in go_mod.lines() {
        let line = line.split("//").next().unwrap_or("").trim();
        let spec = if in_block {
            if line == ")" {
                in_block = false;
                continue;
            }
            line
        } else if let Some(rest) = line.strip_prefix("replace") {
            if rest.trim() == "(" {
                in_block = true;
                continue;
            }
            rest.trim()
        } else {
            continue;
        };
        let Some((left, right)) = spec.split_once("=>") else {
            continue;
        };
        let prefix = left.split_whitespace().next().unwrap_or("");
        let target = right.split_whitespace().next().unwrap_or("");
        if !prefix.is_empty() && !target.is_empty() {
            out.push((
                prefix.trim_matches('"').to_string(),
                target.trim_matches('"').to_string(),
            ));
        }
    }
    out
}

/// A `replace` target that is a relative path (`./x`, `../x`), as a
/// repo-relative directory; `None` for a module path or a path leaving the repo.
fn relative_target(module_dir: &str, target: &str) -> Option<String> {
    if !(target.starts_with("./") || target.starts_with("../") || target == "." || target == "..") {
        return None;
    }
    let mut parts: Vec<&str> = module_dir.split('/').filter(|p| !p.is_empty()).collect();
    for part in target.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            other => parts.push(other),
        }
    }
    Some(parts.join("/"))
}

fn read_package_name(dir: &Path) -> Option<String> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .ok()?
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".go"))
        .collect();
    files.sort();
    let (tests, prod): (Vec<_>, Vec<_>) = files.into_iter().partition(|n| n.ends_with("_test.go"));
    prod.iter()
        .chain(tests.iter())
        .find_map(|name| {
            let text = std::fs::read_to_string(dir.join(name)).ok()?;
            package_clause(&text)
        })
        .map(|n| n.strip_suffix("_test").unwrap_or(&n).to_string())
}

/// The name in a file's `package` clause: the first code line that starts
/// with `package`, past comments (including a leading block comment).
fn package_clause(text: &str) -> Option<String> {
    let mut in_block = false;
    for line in text.lines() {
        let mut line = line.trim();
        if in_block {
            match line.split_once("*/") {
                Some((_, rest)) => {
                    in_block = false;
                    line = rest.trim();
                }
                None => continue,
            }
        }
        if line.starts_with("//") || line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("/*") {
            if let Some((_, after)) = rest.split_once("*/") {
                line = after.trim();
            } else {
                in_block = true;
                continue;
            }
        }
        if let Some(rest) = line.strip_prefix("package")
            && rest.starts_with(char::is_whitespace)
        {
            let name = rest
                .split(|c: char| c.is_whitespace() || c == '/')
                .find(|s| !s.is_empty())?;
            return Some(name.to_string());
        }
        // Past the header without a package clause: not a Go source file.
        if !line.is_empty() && !line.starts_with("package") {
            return None;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_module_and_replace_forms() {
        let text = "module example.com/m // c\n\nreplace a/b v1.0.0 => ../x\nreplace (\n\tc/d => ./vendored/d\n\te/f => github.com/g/h v1.2.3\n)\n";
        assert_eq!(module_path(text).as_deref(), Some("example.com/m"));
        assert_eq!(
            replace_directives(text),
            [
                ("a/b".to_string(), "../x".to_string()),
                ("c/d".to_string(), "./vendored/d".to_string()),
                ("e/f".to_string(), "github.com/g/h".to_string()),
            ]
        );
        assert_eq!(relative_target("sub/m", "../x").as_deref(), Some("sub/x"));
        assert_eq!(relative_target("m", "../../x"), None);
        assert_eq!(relative_target("m", "github.com/g/h"), None);
    }

    #[test]
    fn resolves_imports_across_modules_replaces_and_vendor() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let write = |rel: &str, text: &str| {
            let path = root.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        };
        write("go.mod", "module example.com/m\n");
        write(
            "tools/go.mod",
            "module example.com/tools\nreplace example.com/dep => ../dep\n",
        );
        write("dep/sub/x.go", "package renamed\n");
        write("vendor/github.com/v/lib/l.go", "package lib\n");
        // A sample module repeating the root's path, under testdata.
        write("sample/testdata/go.mod", "module example.com/m\n");
        let repo = GoRepo::scan(root);
        let at = |from: &str, path: &str| repo.resolve_import(root, from, path);
        let in_repo = |d: &str| ImportTarget::InRepo(d.to_string());
        // The root module's packages, imported from the nested module.
        assert_eq!(at("tools/a.go", "example.com/m/x/y"), in_repo("x/y"));
        assert_eq!(at("tools/a.go", "example.com/m"), in_repo(""));
        // The nested module's own path, and a replaced path outside any module.
        assert_eq!(at("a.go", "example.com/tools/p"), in_repo("tools/p"));
        assert_eq!(at("tools/a.go", "example.com/dep/sub"), in_repo("dep/sub"));
        // Nor does a module that only repeats the root's path win.
        write("tools/dup/go.mod", "module example.com/m\n");
        let repo2 = GoRepo::scan(root);
        assert_eq!(
            repo2.resolve_import(root, "a.go", "example.com/m/x"),
            in_repo("x")
        );
        // A prefix must end at a path element.
        assert_eq!(at("a.go", "example.com/mm/x"), ImportTarget::Foreign);
        // Vendored, and plain foreign.
        assert_eq!(
            at("a.go", "github.com/v/lib"),
            in_repo("vendor/github.com/v/lib")
        );
        assert_eq!(at("a.go", "fmt"), ImportTarget::Foreign);
        // The declared name, not the directory's.
        assert_eq!(
            repo.package_name(root, "dep/sub").as_deref(),
            Some("renamed")
        );
        assert_eq!(repo.package_name(root, "nowhere"), None);
    }

    #[test]
    fn reads_the_package_clause_past_comments() {
        assert_eq!(
            package_clause("// Package x does.\n/* block\n more */\npackage x // c\n").as_deref(),
            Some("x")
        );
        assert_eq!(package_clause("package main\n").as_deref(), Some("main"));
        assert_eq!(package_clause("func f() {}\n"), None);
    }
}
