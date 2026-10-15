//! Monorepo workspace packages imported by their package name
//! (`import {x} from '@scope/pkg/sub'`): a bare specifier whose package name
//! is the `name` of a `package.json` in the repository names that package's
//! source, found through `exports`, `types`/`main` or the `src` convention.
//!
//! Which manifests count: when the repository declares workspaces (root
//! `package.json` `workspaces`, `pnpm-workspace.yaml`, `lerna.json`) only the
//! declared members do; otherwise every manifest outside fixture, example and
//! test directories. A target must be a JS/TS file the scan walk lists, so
//! ignored build output, symlinks and anything outside the repository never
//! resolve. Resolution follows Node: with `exports` present only it decides.

use crate::indexer::javascript::is_js_ts_path;
use crate::indexer::scan::{ScanOptions, walker_from};
use serde::Deserialize;
use serde::de::{self, Deserializer, MapAccess, SeqAccess, Visitor};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;

/// Manifests larger than this are skipped.
const MAX_MANIFEST_BYTES: u64 = 1_000_000;
/// Output directories a package builds into, mapped back to `src`.
const BUILD_DIRS: &[&str] = &[
    "dist", "build", "lib", "out", "esm", "cjs", "esm5", "esm2015",
];
/// Emitted-file suffixes, longest first, stripped to reach the source stem.
const EMITTED_SUFFIXES: &[&str] = &[
    ".d.mts", ".d.cts", ".d.ts", ".mts", ".cts", ".mjs", ".cjs", ".jsx", ".tsx", ".js", ".ts",
];
/// Directory names whose manifests are fixtures, not workspace packages, in a
/// repository that declares no workspaces.
const NON_PACKAGE_DIRS: &[&str] = &[
    "fixtures",
    "fixture",
    "__fixtures__",
    "testdata",
    "examples",
    "example",
    "test",
    "tests",
    "__tests__",
];
/// Condition keys in the order TypeScript resolution prefers them.
const CONDITION_PRIORITY: &[&str] = &["types", "import", "default", "require"];

/// A JSON value whose objects keep their document order.
#[derive(Debug, Clone)]
enum Json {
    Null,
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
    Other,
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Json;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any JSON value")
            }
            fn visit_unit<E: de::Error>(self) -> Result<Json, E> {
                Ok(Json::Null)
            }
            fn visit_bool<E: de::Error>(self, _: bool) -> Result<Json, E> {
                Ok(Json::Other)
            }
            fn visit_i64<E: de::Error>(self, _: i64) -> Result<Json, E> {
                Ok(Json::Other)
            }
            fn visit_u64<E: de::Error>(self, _: u64) -> Result<Json, E> {
                Ok(Json::Other)
            }
            fn visit_f64<E: de::Error>(self, _: f64) -> Result<Json, E> {
                Ok(Json::Other)
            }
            fn visit_str<E: de::Error>(self, s: &str) -> Result<Json, E> {
                Ok(Json::Str(s.to_string()))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Json, A::Error> {
                let mut out = Vec::new();
                while let Some(v) = a.next_element()? {
                    out.push(v);
                }
                Ok(Json::Arr(out))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Json, A::Error> {
                let mut out = Vec::new();
                while let Some((k, v)) = a.next_entry::<String, Json>()? {
                    out.push((k, v));
                }
                Ok(Json::Obj(out))
            }
        }
        d.deserialize_any(V)
    }
}

impl Json {
    fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Obj(items) => items.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }

    fn strings(&self) -> Vec<&str> {
        match self {
            Json::Str(s) => vec![s],
            Json::Arr(items) => items.iter().filter_map(Json::as_str).collect(),
            _ => Vec::new(),
        }
    }

    /// Compact text for fingerprints.
    fn render(&self, out: &mut String) {
        match self {
            Json::Null => out.push_str("null"),
            Json::Other => out.push('~'),
            Json::Str(s) => {
                out.push('"');
                out.push_str(s);
                out.push('"');
            }
            Json::Arr(items) => {
                out.push('[');
                items.iter().for_each(|v| {
                    v.render(out);
                    out.push(',');
                });
                out.push(']');
            }
            Json::Obj(items) => {
                out.push('{');
                for (k, v) in items {
                    out.push_str(k);
                    out.push(':');
                    v.render(out);
                    out.push(',');
                }
                out.push('}');
            }
        }
    }
}

struct Package {
    dir: PathBuf,
    manifest: Json,
}

struct Workspace {
    packages: HashMap<String, Vec<Rc<Package>>>,
    /// Every non-symlink JS/TS file the scan walk lists, repo-relative.
    files: HashSet<String>,
}

thread_local! {
    static WORKSPACES: RefCell<HashMap<PathBuf, Rc<Workspace>>> = RefCell::new(HashMap::new());
}

/// Drops the cached tables (batch boundary, see `clear_export_cache`).
pub(crate) fn clear_cache() {
    WORKSPACES.with(|c| c.borrow_mut().clear());
}

fn read_manifest(path: &Path) -> Option<Json> {
    let meta = std::fs::metadata(path).ok()?;
    if meta.len() > MAX_MANIFEST_BYTES {
        return None;
    }
    serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
}

/// The manifest fields that decide resolution, as text for the reindex
/// fingerprint (an edit to dependencies or scripts must not change it).
pub(crate) fn manifest_summary(path: &Path) -> Option<String> {
    let manifest = read_manifest(path)?;
    let mut out = String::new();
    for key in [
        "name",
        "exports",
        "types",
        "typings",
        "module",
        "main",
        "workspaces",
    ] {
        if let Some(v) = manifest.get(key) {
            out.push_str(key);
            out.push('=');
            v.render(&mut out);
            out.push(';');
        }
    }
    Some(out)
}

fn workspace(repo_root: &Path) -> Rc<Workspace> {
    if let Some(found) = WORKSPACES.with(|c| c.borrow().get(repo_root).cloned()) {
        return found;
    }
    let mut manifests: Vec<(PathBuf, Json)> = Vec::new();
    let mut files = HashSet::new();
    for entry in walker_from(repo_root, ScanOptions::default(), None).flatten() {
        let path = entry.path();
        let Some(file_type) = entry.file_type() else {
            continue;
        };
        if !file_type.is_file() || path.components().any(|c| c.as_os_str() == "node_modules") {
            continue;
        }
        let Ok(rel) = path.strip_prefix(repo_root) else {
            continue;
        };
        let rel_text = crate::util::normalize_path(rel);
        if is_js_ts_path(&rel_text) {
            files.insert(rel_text);
        } else if path.file_name().is_some_and(|n| n == "package.json")
            && let Some(manifest) = read_manifest(path)
        {
            manifests.push((
                rel.parent().map(Path::to_path_buf).unwrap_or_default(),
                manifest,
            ));
        }
    }
    let declared = declared_members(repo_root, &manifests);
    let mut packages: HashMap<String, Vec<Rc<Package>>> = HashMap::new();
    for (dir, manifest) in manifests {
        let counts = match &declared {
            Some(globs) => dir.as_os_str().is_empty() || globs.matches(&dir),
            None => !dir
                .components()
                .any(|c| NON_PACKAGE_DIRS.contains(&c.as_os_str().to_string_lossy().as_ref())),
        };
        let Some(name) = manifest.get("name").and_then(Json::as_str) else {
            continue;
        };
        if counts {
            let name = name.to_string();
            packages
                .entry(name)
                .or_default()
                .push(Rc::new(Package { dir, manifest }));
        }
    }
    let ws = Rc::new(Workspace { packages, files });
    WORKSPACES.with(|c| c.borrow_mut().insert(repo_root.to_path_buf(), ws.clone()));
    ws
}

/// Workspace member globs the repository declares, if it declares any.
struct MemberGlobs {
    include: Vec<Vec<String>>,
    exclude: Vec<Vec<String>>,
}

impl MemberGlobs {
    fn matches(&self, dir: &Path) -> bool {
        let segments: Vec<String> = dir
            .components()
            .filter_map(|c| match c {
                Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
                _ => None,
            })
            .collect();
        self.include.iter().any(|g| glob_match(g, &segments))
            && !self.exclude.iter().any(|g| glob_match(g, &segments))
    }
}

fn declared_members(repo_root: &Path, manifests: &[(PathBuf, Json)]) -> Option<MemberGlobs> {
    let mut patterns: Vec<String> = Vec::new();
    let mut declared = false;
    if let Some((_, root)) = manifests.iter().find(|(d, _)| d.as_os_str().is_empty())
        && let Some(ws) = root.get("workspaces")
    {
        declared = true;
        let list = match ws {
            Json::Obj(_) => ws.get("packages"),
            other => Some(other),
        };
        patterns.extend(list.into_iter().flat_map(Json::strings).map(str::to_string));
    }
    if let Ok(text) = std::fs::read_to_string(repo_root.join("pnpm-workspace.yaml")) {
        declared = true;
        if let Ok(value) = serde_yaml_ng::from_str::<serde_yaml_ng::Value>(&text)
            && let Some(list) = value.get("packages").and_then(|v| v.as_sequence())
        {
            patterns.extend(list.iter().filter_map(|v| v.as_str()).map(str::to_string));
        }
    }
    if let Some(lerna) = std::fs::read_to_string(repo_root.join("lerna.json"))
        .ok()
        .and_then(|t| serde_json::from_str::<Json>(&t).ok())
    {
        declared = true;
        match lerna.get("packages") {
            Some(list) => patterns.extend(list.strings().into_iter().map(str::to_string)),
            None => patterns.push("packages/*".to_string()),
        }
    }
    if !declared {
        return None;
    }
    let split = |p: &str| -> Vec<String> {
        p.trim_start_matches("./")
            .trim_end_matches('/')
            .split('/')
            .map(str::to_string)
            .collect()
    };
    let mut out = MemberGlobs {
        include: Vec::new(),
        exclude: Vec::new(),
    };
    for p in &patterns {
        match p.strip_prefix('!') {
            Some(neg) => out.exclude.push(split(neg)),
            None => out.include.push(split(p)),
        }
    }
    Some(out)
}

fn glob_match(pattern: &[String], segments: &[String]) -> bool {
    match pattern.split_first() {
        None => segments.is_empty(),
        Some((p, rest)) if p == "**" => {
            (0..=segments.len()).any(|skip| glob_match(rest, &segments[skip..]))
        }
        Some((p, rest)) => match segments.split_first() {
            Some((s, tail)) => wildcard(p, s) && glob_match(rest, tail),
            None => false,
        },
    }
}

/// `*` within one path segment.
fn wildcard(pattern: &str, text: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern == text,
        Some((head, tail)) => text.strip_prefix(head).is_some_and(|rest| {
            (0..=rest.len())
                .filter(|i| rest.is_char_boundary(*i))
                .any(|i| wildcard(tail, &rest[i..]))
        }),
    }
}

/// Whether the scan walk lists `rel` as a (non-symlink) JS/TS file.
pub(crate) fn is_scanned(repo_root: &Path, rel: &Path) -> bool {
    workspace(repo_root)
        .files
        .contains(&crate::util::normalize_path(rel))
}

/// Names of the workspace packages whose directory contains `rel`.
pub(crate) fn owning_package_names(repo_root: &Path, rel: &str) -> Vec<String> {
    let ws = workspace(repo_root);
    let path = Path::new(rel);
    ws.packages
        .iter()
        .filter(|(_, pkgs)| pkgs.iter().any(|p| path.starts_with(&p.dir)))
        .map(|(name, _)| name.clone())
        .collect()
}

/// Repo-relative, extension-less module stems `spec` may name, best first.
/// Empty when `spec` names no workspace package (or a name two packages
/// share, which no import can be attributed to).
pub(crate) fn module_stems(repo_root: &Path, spec: &str) -> Vec<PathBuf> {
    let ws = workspace(repo_root);
    let Some((name, sub)) = split_package(spec, &ws.packages) else {
        return Vec::new();
    };
    let [package] = &ws.packages[name][..] else {
        return Vec::new();
    };
    if !is_safe(sub) {
        return Vec::new();
    }
    let mut stems: Vec<PathBuf> = Vec::new();
    let mut push = |p: PathBuf| {
        if !stems.contains(&p) {
            stems.push(p);
        }
    };
    if let Some(exports) = package.manifest.get("exports") {
        // Node: `exports` alone decides; an unlisted subpath does not exist.
        for target in export_targets(exports, sub) {
            for stem in source_stems(&target) {
                push(package.dir.join(stem));
            }
        }
        return stems;
    }
    if sub.is_empty() {
        for field in ["types", "typings", "module", "main"] {
            if let Some(target) = package.manifest.get(field).and_then(Json::as_str) {
                for stem in source_stems(target) {
                    push(package.dir.join(stem));
                }
            }
        }
        push(package.dir.join("src").join("index"));
        push(package.dir.join("index"));
    } else {
        push(package.dir.join("src").join(sub));
        push(package.dir.join(sub));
    }
    stems
}

/// A relative path with no `..`, root or prefix components.
fn is_safe(path: &str) -> bool {
    Path::new(path)
        .components()
        .all(|c| matches!(c, Component::Normal(_) | Component::CurDir))
}

/// The longest package name that is `spec` or a prefix of it before a `/`,
/// with the subpath after it.
fn split_package<'a>(
    spec: &'a str,
    table: &'a HashMap<String, Vec<Rc<Package>>>,
) -> Option<(&'a str, &'a str)> {
    let mut end = spec.len();
    loop {
        let name = &spec[..end];
        if table.contains_key(name) {
            return Some((name, spec[end..].trim_start_matches('/')));
        }
        end = name.rfind('/')?;
    }
}

/// Every path string `exports` gives for `sub`, best condition first.
fn export_targets(exports: &Json, sub: &str) -> Vec<String> {
    let key = if sub.is_empty() {
        ".".to_string()
    } else {
        format!("./{sub}")
    };
    let value: Option<(&Json, Option<String>)> = match exports {
        Json::Obj(map) if map.iter().any(|(k, _)| k.starts_with('.')) => {
            if let Some((_, v)) = map.iter().find(|(k, _)| *k == key) {
                Some((v, None))
            } else {
                // The longest matching prefix wins, as in Node.
                map.iter()
                    .filter_map(|(pattern, v)| {
                        let (head, tail) = pattern.split_once('*')?;
                        let mid = key.strip_prefix(head)?.strip_suffix(tail)?;
                        Some((head.len(), v, mid.to_string()))
                    })
                    .max_by_key(|(len, _, _)| *len)
                    .map(|(_, v, mid)| (v, Some(mid)))
            }
        }
        other if sub.is_empty() => Some((other, None)),
        _ => None,
    };
    let mut out = Vec::new();
    if let Some((v, mid)) = value {
        collect_targets(v, mid.as_deref(), &mut out);
    }
    out
}

fn collect_targets(value: &Json, star: Option<&str>, out: &mut Vec<String>) {
    match value {
        Json::Str(s) => out.push(match star {
            Some(mid) => s.replace('*', mid),
            None => s.clone(),
        }),
        Json::Arr(items) => items.iter().for_each(|v| collect_targets(v, star, out)),
        Json::Obj(map) => {
            for key in CONDITION_PRIORITY {
                if let Some((_, v)) = map.iter().find(|(k, _)| k == key) {
                    collect_targets(v, star, out);
                }
            }
            for (key, v) in map {
                if !CONDITION_PRIORITY.contains(&key.as_str()) {
                    collect_targets(v, star, out);
                }
            }
        }
        _ => {}
    }
}

/// The source stems an emitted/declared path (`./dist/a/index.d.mts`) may
/// stand for: the path under `src`, then as written, without its suffix.
/// Paths with `..` or absolute components stand for nothing.
fn source_stems(target: &str) -> Vec<PathBuf> {
    let trimmed = target.trim_start_matches("./");
    if !is_safe(trimmed) {
        return Vec::new();
    }
    let stem = EMITTED_SUFFIXES
        .iter()
        .find_map(|s| trimmed.strip_suffix(s))
        .unwrap_or(trimmed);
    let mut out = Vec::new();
    if let Some((first, rest)) = stem.split_once('/')
        && BUILD_DIRS.contains(&first)
    {
        out.push(Path::new("src").join(rest));
    }
    out.push(PathBuf::from(stem));
    out
}
