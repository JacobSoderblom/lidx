//! Public-API classification for `dead_symbols` (issues #461, #462).
//!
//! A declaration other code may use -- an exported Go identifier of an
//! importable package, a `pub` item of a Rust library crate, what a
//! TypeScript package's entry exports -- cannot be proven dead from inside
//! the repository. Each language's rule is its `LanguageProfile::public_api`;
//! this module applies it to one candidate at a time, holding the per-package
//! facts (`package.json` entries, library crates) it needs.

use super::graph_query::has_modifier;
use super::resolver::{PublicApiRule, profile_for};
use crate::indexer::go::GO_PACKAGE_MAIN;
use crate::indexer::javascript;
use std::collections::{HashMap, HashSet};
use std::path::Path;

/// What `dead_symbols` knows of one candidate declaration.
pub(crate) struct Candidate<'a> {
    pub language: &'a str,
    pub path: &'a str,
    pub kind: &'a str,
    pub name: &'a str,
    pub qualname: &'a str,
    pub visibility: Option<&'a str>,
    /// Concatenated `visibility` of the symbols named like this one's
    /// parent (methods only).
    pub parent_visibility: Option<&'a str>,
}

/// How exposed a declaration is.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Exposure {
    /// Other code may use it: reported only on request.
    PublicApi(String),
    /// Modelled and not public: judged on its references alone.
    Internal(String),
    /// Exported, but from somewhere nobody can import it (`package main`,
    /// an `internal` directory, a package without a library entry).
    ExportedUnimportable(String),
    /// The language's visibility is not modelled.
    Unmodelled,
}

/// The package facts the rules need, read lazily.
pub(crate) struct PublicApi<'a> {
    repo_root: &'a Path,
    /// Indexed JS/TS paths (for mapping `package.json` entries to sources).
    js_files: HashSet<String>,
    /// Directories holding a Rust crate with a library target.
    rust_libs: HashSet<String>,
    /// Package directory -> its exported declarations (`None`: no entry
    /// mapped to an indexed file).
    packages: HashMap<String, Option<HashSet<(String, String)>>>,
    /// Directory -> nearest enclosing `package.json` directory.
    nearest: HashMap<String, Option<String>>,
}

impl<'a> PublicApi<'a> {
    pub(crate) fn new(
        repo_root: &'a Path,
        js_files: HashSet<String>,
        rust_lib_roots: HashSet<String>,
    ) -> Self {
        let rust_libs = rust_lib_roots
            .into_iter()
            .map(|lib| {
                lib.trim_end_matches("src/lib.rs")
                    .trim_end_matches('/')
                    .to_string()
            })
            .collect();
        PublicApi {
            repo_root,
            js_files,
            rust_libs,
            packages: HashMap::new(),
            nearest: HashMap::new(),
        }
    }

    pub(crate) fn classify(&mut self, c: &Candidate<'_>) -> Exposure {
        let profile = profile_for(c.language);
        let private = has_modifier(c.visibility, "private");
        match profile.public_api {
            PublicApiRule::None => {
                if private {
                    Exposure::Internal("private".into())
                } else {
                    Exposure::Unmodelled
                }
            }
            PublicApiRule::GoExported => self.go(c),
            PublicApiRule::RustPub => self.rust(c, private),
            PublicApiRule::EntryExports => self.entry_exports(c, private),
        }
    }

    fn go(&self, c: &Candidate<'_>) -> Exposure {
        let exported_name = |n: &str| n.chars().next().is_some_and(char::is_uppercase);
        if !exported_name(c.name) {
            return Exposure::Internal("unexported".into());
        }
        // An exported method of an unexported type is API too: it satisfies
        // interfaces (`sort.Interface`, `error`, `fmt.Stringer`) callers
        // outside the package hold.
        if has_modifier(c.visibility, GO_PACKAGE_MAIN) {
            return Exposure::ExportedUnimportable("exported, but in package main".into());
        }
        if c.path.ends_with("_test.go") {
            return Exposure::ExportedUnimportable("exported, but in a _test.go file".into());
        }
        let dir = c.path.rsplit_once('/').map_or("", |(d, _)| d);
        if dir.split('/').any(|seg| seg == "internal") {
            return Exposure::ExportedUnimportable("exported, but in an internal package".into());
        }
        Exposure::PublicApi("exported identifier of an importable Go package".into())
    }

    fn rust(&self, c: &Candidate<'_>, private: bool) -> Exposure {
        if private {
            return Exposure::Internal("private (no pub)".into());
        }
        if !has_modifier(c.visibility, "public") {
            return Exposure::Internal("restricted visibility (pub(crate) or narrower)".into());
        }
        if c.kind == "method" && !has_modifier(c.parent_visibility, "public") {
            return Exposure::Internal("pub method of a type that is not pub".into());
        }
        let package = (profile_for("rust").package_dir).map_or("", |package_of| package_of(c.path));
        if !self.rust_libs.contains(package) {
            return Exposure::Internal("pub item of a binary crate".into());
        }
        let file = c.path.rsplit('/').next().unwrap_or("");
        if file == "main.rs" || c.path.contains("/src/bin/") || c.path.starts_with("src/bin/") {
            return Exposure::Internal("pub item of a binary target".into());
        }
        Exposure::PublicApi("pub item of a library crate".into())
    }

    fn entry_exports(&mut self, c: &Candidate<'_>, private: bool) -> Exposure {
        if private {
            return Exposure::Internal("not exported".into());
        }
        let Some(package) = self.package_of(c.path) else {
            return Exposure::PublicApi("exported (no package.json to say otherwise)".into());
        };
        let top = javascript::top_level_name(c.path, c.qualname).unwrap_or(c.name);
        match self.surface(&package) {
            Some(surface) if surface.contains(&(c.path.to_string(), top.to_string())) => {
                Exposure::PublicApi("exported from the package entry".into())
            }
            Some(_) => Exposure::ExportedUnimportable(
                "exported, but not reachable from the package entry".into(),
            ),
            None => Exposure::ExportedUnimportable(
                "exported, but its package.json names no library entry".into(),
            ),
        }
    }

    /// The directory of the nearest `package.json` at or above `path`.
    fn package_of(&mut self, path: &str) -> Option<String> {
        let dir = path.rsplit_once('/').map_or("", |(d, _)| d).to_string();
        if let Some(hit) = self.nearest.get(&dir) {
            return hit.clone();
        }
        let mut cursor = dir.clone();
        let found = loop {
            if self.repo_root.join(&cursor).join("package.json").is_file() {
                break Some(cursor.clone());
            }
            match cursor.rsplit_once('/') {
                Some((parent, _)) => cursor = parent.to_string(),
                None if !cursor.is_empty() => cursor = String::new(),
                None => break None,
            }
        };
        self.nearest.insert(dir, found.clone());
        found
    }

    fn surface(&mut self, package: &str) -> Option<&HashSet<(String, String)>> {
        if !self.packages.contains_key(package) {
            let entries = package_entries(self.repo_root, package, &self.js_files);
            let surface = (!entries.is_empty())
                .then(|| javascript::public_api_declarations(self.repo_root, &entries));
            self.packages.insert(package.to_string(), surface);
        }
        self.packages.get(package).and_then(|s| s.as_ref())
    }
}

/// Directory names a build tool writes entry files to, whose sources live
/// elsewhere (`dist/index.js` is built from `src/index.ts`).
const BUILD_DIRS: &[&str] = &[
    "dist",
    "distribution",
    "lib",
    "build",
    "esm",
    "cjs",
    "es",
    "out",
    "types",
    "module",
    "mjs",
    "cts",
    "mts",
];

/// The indexed source files a package's `package.json` entry fields
/// (`main`, `module`, `types`, `typings`, `browser`, `bin`, `exports`) name.
fn package_entries(repo_root: &Path, package: &str, files: &HashSet<String>) -> Vec<String> {
    let manifest = repo_root.join(package).join("package.json");
    let Ok(text) = std::fs::read_to_string(&manifest) else {
        return Vec::new();
    };
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) else {
        return Vec::new();
    };
    let mut targets = Vec::new();
    for key in [
        "main", "module", "types", "typings", "browser", "bin", "exports",
    ] {
        if let Some(v) = json.get(key) {
            leaf_strings(v, &mut targets);
        }
    }
    let mut found: Vec<String> = Vec::new();
    for target in targets {
        for file in map_target(package, &target, files) {
            if !found.contains(&file) {
                found.push(file);
            }
        }
    }
    found.sort();
    found
}

fn leaf_strings(v: &serde_json::Value, out: &mut Vec<String>) {
    match v {
        serde_json::Value::String(s) => out.push(s.clone()),
        serde_json::Value::Array(a) => a.iter().for_each(|x| leaf_strings(x, out)),
        serde_json::Value::Object(o) => o.values().for_each(|x| leaf_strings(x, out)),
        _ => {}
    }
}

const SOURCE_SUFFIXES: &[&str] = &[
    ".ts",
    ".tsx",
    ".mts",
    ".cts",
    "/index.ts",
    "/index.tsx",
    ".js",
    ".jsx",
    ".mjs",
    ".cjs",
    "/index.js",
];

/// The indexed source file(s) an entry-field string `target` (relative to
/// the package directory `package`) stands for.
fn map_target(package: &str, target: &str, files: &HashSet<String>) -> Vec<String> {
    let rel = target.trim_start_matches("./");
    if [".json", ".node", ".css"].iter().any(|e| rel.ends_with(e)) {
        return Vec::new();
    }
    let stem = ["d.ts", "ts", "tsx", "mts", "cts", "js", "jsx", "mjs", "cjs"]
        .iter()
        .find_map(|ext| rel.strip_suffix(&format!(".{ext}")))
        .map(|s| s.strip_suffix(".d").unwrap_or(s))
        .unwrap_or(rel);
    let segs: Vec<&str> = stem.split('/').collect();
    let join = |base: &str, v: &str| {
        let mut p = String::new();
        if !package.is_empty() {
            p.push_str(package);
            p.push('/');
        }
        p.push_str(base);
        p.push_str(v);
        p
    };
    let mut out = Vec::new();
    for i in 0..segs.len() {
        if i > 0 && !BUILD_DIRS.contains(&segs[i - 1]) {
            break;
        }
        let variant = segs[i..].join("/");
        for base in ["", "src/"] {
            let prefix = join(base, &variant);
            if let Some((head, tail)) = prefix.split_once('*') {
                for f in files {
                    for suffix in SOURCE_SUFFIXES {
                        if let Some(mid) = f
                            .strip_prefix(head)
                            .and_then(|r| r.strip_suffix(&format!("{tail}{suffix}")))
                            && !mid.is_empty()
                            && !mid.contains('/')
                        {
                            out.push(f.clone());
                            break;
                        }
                    }
                }
            } else if let Some(hit) = SOURCE_SUFFIXES
                .iter()
                .map(|s| format!("{prefix}{s}"))
                .find(|cand| files.contains(cand))
            {
                out.push(hit);
            }
            if !out.is_empty() {
                return out;
            }
        }
    }
    out
}
