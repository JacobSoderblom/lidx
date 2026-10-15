//! Monorepo workspace packages imported by their package name
//! (`import {x} from '@scope/pkg/sub'`): a bare specifier whose package name
//! is the `name` of a `package.json` in the repository names that package's
//! source, found through `exports`, `types`/`main` or the `src` convention.

use crate::indexer::scan::{ScanOptions, walker_from};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;

/// Output directories a package builds into, mapped back to `src`.
const BUILD_DIRS: &[&str] = &[
    "dist", "build", "lib", "out", "esm", "cjs", "esm5", "esm2015",
];
/// Emitted-file suffixes, longest first, stripped to reach the source stem.
const EMITTED_SUFFIXES: &[&str] = &[
    ".d.mts", ".d.cts", ".d.ts", ".mts", ".cts", ".mjs", ".cjs", ".jsx", ".tsx", ".js", ".ts",
];

struct Package {
    dir: PathBuf,
    manifest: Value,
}

type Packages = HashMap<String, Vec<Rc<Package>>>;

thread_local! {
    static PACKAGES: RefCell<HashMap<PathBuf, Rc<Packages>>> = RefCell::new(HashMap::new());
}

/// Drops the cached package table (batch boundary, see `clear_export_cache`).
pub(crate) fn clear_cache() {
    PACKAGES.with(|c| c.borrow_mut().clear());
}

/// The fields of the manifest at `path` that decide resolution, as text.
pub(crate) fn manifest_summary(path: &Path) -> Option<String> {
    let manifest: Value = serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()?;
    let picked: serde_json::Map<String, Value> =
        ["name", "exports", "types", "typings", "module", "main"]
            .iter()
            .filter_map(|k| manifest.get(*k).map(|v| (k.to_string(), v.clone())))
            .collect();
    Some(Value::Object(picked).to_string())
}

fn packages(repo_root: &Path) -> Rc<Packages> {
    if let Some(found) = PACKAGES.with(|c| c.borrow().get(repo_root).cloned()) {
        return found;
    }
    let mut table: Packages = HashMap::new();
    for entry in walker_from(repo_root, ScanOptions::default(), None).flatten() {
        let path = entry.path();
        if path.file_name().is_none_or(|n| n != "package.json")
            || path.components().any(|c| c.as_os_str() == "node_modules")
        {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(path) else {
            continue;
        };
        let Ok(manifest) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        let Some(name) = manifest.get("name").and_then(Value::as_str) else {
            continue;
        };
        let dir = path
            .parent()
            .and_then(|d| d.strip_prefix(repo_root).ok())
            .map(Path::to_path_buf)
            .unwrap_or_default();
        table
            .entry(name.to_string())
            .or_default()
            .push(Rc::new(Package { dir, manifest }));
    }
    let table = Rc::new(table);
    PACKAGES.with(|c| {
        c.borrow_mut()
            .insert(repo_root.to_path_buf(), table.clone())
    });
    table
}

/// Repo-relative, extension-less module stems `spec` may name, best first.
/// Empty when `spec` names no workspace package (or a name two packages
/// share, which no import can be attributed to).
pub(crate) fn module_stems(repo_root: &Path, spec: &str) -> Vec<PathBuf> {
    let table = packages(repo_root);
    let Some((name, sub)) = split_package(spec, &table) else {
        return Vec::new();
    };
    let [package] = &table[name][..] else {
        return Vec::new();
    };
    let mut stems = Vec::new();
    let mut push = |p: PathBuf| {
        if !stems.contains(&p) {
            stems.push(p);
        }
    };
    for target in export_targets(&package.manifest, sub) {
        for stem in source_stems(&target) {
            push(package.dir.join(stem));
        }
    }
    if sub.is_empty() {
        for field in ["types", "typings", "module", "main"] {
            if let Some(target) = package.manifest.get(field).and_then(Value::as_str) {
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

/// The longest package name that is `spec` or a prefix of it before a `/`,
/// with the subpath after it.
fn split_package<'a>(spec: &'a str, table: &'a Packages) -> Option<(&'a str, &'a str)> {
    let mut end = spec.len();
    loop {
        let name = &spec[..end];
        if table.contains_key(name) {
            return Some((name, spec[end..].trim_start_matches('/')));
        }
        end = name.rfind('/')?;
    }
}

/// Every path string `exports` gives for `sub`, in condition order with type
/// declarations first.
fn export_targets(manifest: &Value, sub: &str) -> Vec<String> {
    let Some(exports) = manifest.get("exports") else {
        return Vec::new();
    };
    let key = if sub.is_empty() {
        ".".to_string()
    } else {
        format!("./{sub}")
    };
    let value = match exports {
        Value::Object(map) if map.keys().any(|k| k.starts_with('.')) => {
            if let Some(v) = map.get(&key) {
                v.clone()
            } else {
                match map.iter().find_map(|(pattern, v)| {
                    let (head, tail) = pattern.split_once('*')?;
                    let mid = key.strip_prefix(head)?.strip_suffix(tail)?;
                    Some((v, mid))
                }) {
                    Some((v, mid)) => substitute_star(v, mid),
                    None => return Vec::new(),
                }
            }
        }
        other if sub.is_empty() => other.clone(),
        _ => return Vec::new(),
    };
    let mut out = Vec::new();
    collect_strings(&value, &mut out);
    out
}

fn substitute_star(value: &Value, mid: &str) -> Value {
    match value {
        Value::String(s) => Value::String(s.replace('*', mid)),
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, v)| (k.clone(), substitute_star(v, mid)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn collect_strings(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::String(s) => out.push(s.clone()),
        Value::Array(items) => items.iter().for_each(|v| collect_strings(v, out)),
        Value::Object(map) => {
            for key in map.keys().filter(|k| *k == "types") {
                collect_strings(&map[key], out);
            }
            for (key, v) in map {
                if key != "types" {
                    collect_strings(v, out);
                }
            }
        }
        _ => {}
    }
}

/// The source stems an emitted/declared path (`./dist/a/index.d.mts`) may
/// stand for: the path under `src`, then as written, without its suffix.
fn source_stems(target: &str) -> Vec<PathBuf> {
    let trimmed = target.trim_start_matches("./");
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
