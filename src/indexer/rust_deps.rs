//! The error type a Rust dependency's function or method returns (the
//! `From` selection of `.map_err(From::from)`), read from sources that are
//! already on disk: the cargo registry (`$CARGO_HOME/registry/src/*/`, else
//! `~/.cargo`) at the version `Cargo.lock` pins, or the repository's own
//! `vendor/<crate>`. Nothing is downloaded, cargo and build scripts are never
//! run, and a crate whose sources are not on disk answers nothing. A crate
//! name is a plain identifier and a locked version a plain semver string (no
//! `/`, `\\` or `..`), and the source directory has to resolve inside the
//! registry, or inside both `vendor/` and the repository.
//!
//! The scan of a crate is keyed by name and version and kept for the life of
//! the process, so a reindex reads a crate once; a crate whose sources are
//! missing is looked for again on every use. A crate's sources are read
//! up to a total byte and file cap, and a `src` that is a symlink is not
//! followed.

use crate::indexer::tree_helpers::node_text;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use tree_sitter::{Node, Parser};

/// A function or method of a dependency.
struct DepFn {
    /// The leaf name of the type of its `impl` block; `None` for a free fn.
    owner: Option<String>,
    name: String,
    /// The return type as written, whitespace collapsed.
    ret: String,
}

/// What the scan of one crate keeps.
struct CrateIndex {
    name: String,
    fns: Vec<DepFn>,
    /// `type Result<T> = result::Result<T, Error>`: alias name to its right side.
    aliases: HashMap<String, String>,
    /// Structs, enums and unions the crate declares (leaf names).
    types: HashSet<String>,
}

type CacheKey = (String, String);

/// Crates read, by name and version. A miss is not remembered: sources that
/// appear later (a registry filled by `cargo fetch`) are seen by a running
/// server, and looking for one costs a few `stat`s.
fn cache() -> &'static Mutex<HashMap<CacheKey, Arc<CrateIndex>>> {
    static CACHE: OnceLock<Mutex<HashMap<CacheKey, Arc<CrateIndex>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// The most source bytes read from one crate, and the most files.
const MAX_CRATE_BYTES: u64 = 32 * 1024 * 1024;
const MAX_CRATE_FILES: usize = 2000;
const MAX_FILE_BYTES: u64 = 4 * 1024 * 1024;

/// A version `Cargo.lock` records that is a plain semver string: it becomes a
/// directory name, so no separator, no `..`.
fn is_safe_version(version: &str) -> bool {
    !version.is_empty()
        && version.len() <= 64
        && !version.contains("..")
        && version
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '+' | '-'))
}

/// `name`'s version pinned by the `Cargo.lock` at `dir`: a registry package
/// only, and only when exactly one version is locked.
fn locked_version(dir: &Path, name: &str) -> Option<String> {
    let text = std::fs::read_to_string(dir.join("Cargo.lock")).ok()?;
    let wanted = [name.to_string(), name.replace('_', "-")];
    let mut versions: Vec<String> = Vec::new();
    for block in text.split("[[package]]").skip(1) {
        let field = |key: &str| {
            block.lines().find_map(|l| {
                l.strip_prefix(key)
                    .and_then(|r| r.trim().strip_prefix('='))
                    .map(|v| v.trim().trim_matches('"').to_string())
            })
        };
        if field("name").is_some_and(|n| wanted.contains(&n))
            && field("source").is_some_and(|s| s.starts_with("registry+"))
            && let Some(version) = field("version")
            && is_safe_version(&version)
            && !versions.contains(&version)
        {
            versions.push(version);
        }
    }
    match versions.as_slice() {
        [only] => Some(only.clone()),
        _ => None,
    }
}

fn is_plain_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// The registry directories sources are unpacked in:
/// `$CARGO_HOME/registry/src/*/`, else `~/.cargo/registry/src/*/`.
fn registry_roots() -> Vec<PathBuf> {
    let cargo_home = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cargo")));
    let mut roots: Vec<PathBuf> = Vec::new();
    if let Some(home) = cargo_home
        && let Ok(entries) = std::fs::read_dir(home.join("registry").join("src"))
    {
        roots.extend(entries.flatten().map(|e| e.path()));
        roots.sort();
    }
    roots
}

/// `candidate` as a crate directory: it exists, its real path lies under
/// `allowed` (a canonical directory), and so does the real path of its
/// `src`, which is not itself a symlink.
fn contained_crate(candidate: &Path, allowed: &[&Path]) -> Option<PathBuf> {
    if !candidate.join("Cargo.toml").is_file() {
        return None;
    }
    let real = candidate.canonicalize().ok()?;
    if !allowed.iter().all(|a| real.starts_with(a)) {
        return None;
    }
    let src = real.join("src");
    let meta = std::fs::symlink_metadata(&src).ok()?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        return None;
    }
    src.canonicalize()
        .ok()
        .filter(|s| s.starts_with(&real))
        .map(|_| real)
}

/// Where the sources of `name` at `version` are, inside one of `registries`
/// or the repository's `vendor/`, never outside the repository for a
/// vendored one.
fn source_dir(
    repo_root: &Path,
    registries: &[PathBuf],
    name: &str,
    version: &str,
) -> Option<PathBuf> {
    if !is_safe_version(version) || !is_plain_name(name) {
        return None;
    }
    let package = format!("{}-{version}", name.replace('_', "-"));
    for root in registries {
        let Ok(real_root) = root.canonicalize() else {
            continue;
        };
        if let Some(found) = contained_crate(&root.join(&package), &[&real_root]) {
            return Some(found);
        }
    }
    let real_repo = repo_root.canonicalize().ok()?;
    let vendor = real_repo.join("vendor").canonicalize().ok()?;
    if !vendor.starts_with(&real_repo) {
        return None;
    }
    contained_crate(&vendor.join(name.replace('_', "-")), &[&vendor, &real_repo])
}

fn collapse(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Leaf name of a type node's path, generics dropped.
fn type_leaf(node: Node<'_>, source: &str) -> Option<String> {
    let text = node_text(node, source);
    let head = text
        .split('<')
        .next()?
        .trim()
        .trim_start_matches('&')
        .trim();
    head.rsplit("::")
        .next()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn scan_file(source: &str, index: &mut CrateIndex, parser: &mut Parser) {
    let Some(tree) = parser.parse(source, None) else {
        return;
    };
    let mut stack = vec![(tree.root_node(), None::<String>)];
    while let Some((node, owner)) = stack.pop() {
        let mut next_owner = owner.clone();
        match node.kind() {
            "impl_item" => {
                next_owner = node
                    .child_by_field_name("type")
                    .and_then(|t| type_leaf(t, source));
            }
            "function_item" => {
                if let (Some(name), Some(ret)) = (
                    node.child_by_field_name("name"),
                    node.child_by_field_name("return_type"),
                ) {
                    index.fns.push(DepFn {
                        owner: owner.clone(),
                        name: node_text(name, source),
                        ret: collapse(&node_text(ret, source)),
                    });
                }
                // Nested items are not API.
                continue;
            }
            "type_item" => {
                if let (Some(name), Some(value)) = (
                    node.child_by_field_name("name"),
                    node.child_by_field_name("type"),
                ) {
                    index
                        .aliases
                        .entry(node_text(name, source))
                        .or_insert_with(|| collapse(&node_text(value, source)));
                }
            }
            "struct_item" | "enum_item" | "union_item" => {
                if let Some(name) = node.child_by_field_name("name") {
                    index.types.insert(node_text(name, source));
                }
            }
            _ => {}
        }
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            stack.push((child, next_owner.clone()));
        }
    }
}

fn load_crate(src: &Path, name: &str) -> Option<CrateIndex> {
    let mut parser = Parser::new();
    parser
        .set_language(&tree_sitter_rust::LANGUAGE.into())
        .ok()?;
    let mut index = CrateIndex {
        name: name.replace('-', "_"),
        fns: Vec::new(),
        aliases: HashMap::new(),
        types: HashSet::new(),
    };
    let start = src.join("src");
    match std::fs::symlink_metadata(&start) {
        Ok(m) if m.is_dir() && !m.file_type().is_symlink() => {}
        _ => return None,
    }
    let mut dirs = vec![start];
    let mut files = 0usize;
    let mut bytes = 0u64;
    while let Some(dir) = dirs.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_symlink() {
                continue;
            }
            if kind.is_dir() {
                dirs.push(path);
            } else if path.extension().is_some_and(|e| e == "rs") {
                files += 1;
                if files > MAX_CRATE_FILES {
                    return Some(index);
                }
                match std::fs::metadata(&path) {
                    Ok(m) if m.len() <= MAX_FILE_BYTES => {
                        bytes += m.len();
                        if bytes > MAX_CRATE_BYTES {
                            return Some(index);
                        }
                    }
                    _ => continue,
                }
                if let Ok(text) = std::fs::read_to_string(&path) {
                    scan_file(&text, &mut index, &mut parser);
                }
            }
        }
    }
    Some(index)
}

/// Top-level `<...>` arguments of `Head<A, B>`: `(head, [A, B])`.
fn split_generic(ty: &str) -> (&str, Vec<&str>) {
    let Some(open) = ty.find('<') else {
        return (ty.trim(), Vec::new());
    };
    let head = ty[..open].trim();
    let inner = ty[open + 1..].trim_end().trim_end_matches('>');
    let mut args = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;
    for (i, c) in inner.char_indices() {
        match c {
            '<' | '(' | '[' => depth += 1,
            '>' | ')' | ']' => depth -= 1,
            ',' if depth == 0 => {
                args.push(inner[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
    }
    let last = inner[start..].trim();
    if !last.is_empty() {
        args.push(last);
    }
    (head, args)
}

/// The error type of `ret` written inside crate `index`, as an absolute path
/// (`docopt::Error`, `std::io::Error`).
fn error_of(ret: &str, index: &CrateIndex, depth: usize) -> Option<String> {
    if depth > 3 {
        return None;
    }
    let (head, args) = split_generic(ret.trim());
    let leaf = head.rsplit("::").next().unwrap_or(head);
    match (head, leaf) {
        ("io::Result" | "std::io::Result", _) => return Some("std::io::Error".into()),
        ("fmt::Result" | "std::fmt::Result", _) => return Some("std::fmt::Error".into()),
        _ => {}
    }
    if leaf == "Result" && args.len() == 2 {
        return qualify(args[1], index);
    }
    // `Result<T>`: the crate's own alias, `type Result<T> = result::Result<T, Error>`.
    let alias = index.aliases.get(leaf)?;
    error_of(alias, index, depth + 1)
}

fn qualify(error: &str, index: &CrateIndex) -> Option<String> {
    let error = error.trim().trim_start_matches('&').trim();
    let head = error.split('<').next()?.trim();
    if head.is_empty() || !head.chars().next()?.is_alphabetic() {
        return None;
    }
    if let Some(rest) = head.strip_prefix("crate::") {
        return Some(format!(
            "{}::{}",
            index.name,
            rest.rsplit("::").next().unwrap_or(rest)
        ));
    }
    if head.starts_with("io::") {
        return Some(format!("std::{head}"));
    }
    if head.starts_with("std::") || head.starts_with("core::") {
        return Some(head.to_string());
    }
    if !head.contains("::") && index.types.contains(head) {
        return Some(format!("{}::{head}", index.name));
    }
    if head.contains("::") {
        return Some(head.to_string());
    }
    None
}

/// The error type of `owner::name` (a free function when `owner` is `None`) of
/// the dependency `krate`, found under `search_dirs` (the directories whose
/// `Cargo.lock` may pin it, repo-relative, the repository root last).
pub fn dependency_error(
    repo_root: &Path,
    search_dirs: &[PathBuf],
    krate: &str,
    owner: Option<&str>,
    name: &str,
) -> Option<String> {
    if !is_plain_name(krate) || !is_plain_name(name) {
        return None;
    }
    let version = search_dirs
        .iter()
        .find_map(|dir| locked_version(&repo_root.join(dir), krate))?;
    let key = (krate.replace('_', "-"), version.clone());
    let cached = cache().lock().ok()?.get(&key).cloned();
    let index = match cached {
        Some(found) => found,
        None => {
            let loaded = Arc::new(
                source_dir(repo_root, &registry_roots(), krate, &version)
                    .and_then(|dir| load_crate(&dir, krate))?,
            );
            cache().lock().ok()?.insert(key, loaded.clone());
            loaded
        }
    };
    let mut found: Option<String> = None;
    for f in index
        .fns
        .iter()
        .filter(|f| f.name == name && f.owner.as_deref() == owner)
    {
        let err = error_of(&f.ret, &index, 0)?;
        match &found {
            Some(prev) if *prev != err => return None,
            _ => found = Some(err),
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index_of(source: &str) -> CrateIndex {
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_rust::LANGUAGE.into())
            .unwrap();
        let mut index = CrateIndex {
            name: "dep".into(),
            fns: Vec::new(),
            aliases: HashMap::new(),
            types: HashSet::new(),
        };
        scan_file(source, &mut index, &mut parser);
        index
    }

    #[test]
    fn reads_the_error_of_a_crates_own_alias_and_of_io_result() {
        let index = index_of(
            "pub enum Error {}\ntype Result<T> = result::Result<T, Error>;\n\
             pub struct Docopt;\nimpl Docopt {\n  pub fn new<S>(usage: S) -> Result<Docopt> { todo!() }\n}\n\
             pub struct Writer<W>(W);\nimpl<W: io::Write> Writer<W> {\n  pub fn flush(&mut self) -> io::Result<()> { Ok(()) }\n\
             pub fn finish(&mut self) -> std::result::Result<(), Box<Error>> { Ok(()) }\n}\n",
        );
        let ret = |owner: &str, name: &str| {
            index
                .fns
                .iter()
                .find(|f| f.owner.as_deref() == Some(owner) && f.name == name)
                .map(|f| f.ret.clone())
                .unwrap()
        };
        assert_eq!(
            error_of(&ret("Docopt", "new"), &index, 0).as_deref(),
            Some("dep::Error")
        );
        assert_eq!(
            error_of(&ret("Writer", "flush"), &index, 0).as_deref(),
            Some("std::io::Error")
        );
        // A boxed error is not named by the repository's impls.
        assert_eq!(
            error_of(&ret("Writer", "finish"), &index, 0).as_deref(),
            None
        );
    }

    fn write_crate(dir: &Path, with_src: bool) {
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("Cargo.toml"), "[package]\nname=\"evil\"\n").unwrap();
        if with_src {
            std::fs::write(
                dir.join("src/lib.rs"),
                "pub struct W;\nimpl W { pub fn go() -> io::Result<()> { Ok(()) } }\n",
            )
            .unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_vendor_directory_does_not_escape_the_repository() {
        let outside = tempfile::tempdir().unwrap();
        write_crate(&outside.path().join("evil"), true);
        let repo = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path(), repo.path().join("vendor")).unwrap();
        assert_eq!(source_dir(repo.path(), &[], "evil", "1.0.0"), None);
        // ... nor a symlinked crate directory inside a real vendor/.
        let repo = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(repo.path().join("vendor")).unwrap();
        std::os::unix::fs::symlink(outside.path().join("evil"), repo.path().join("vendor/evil"))
            .unwrap();
        assert_eq!(source_dir(repo.path(), &[], "evil", "1.0.0"), None);
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_src_directory_is_not_followed() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(outside.path().join("elsewhere")).unwrap();
        std::fs::write(
            outside.path().join("elsewhere/lib.rs"),
            "pub struct W;\nimpl W { pub fn go() -> io::Result<()> { Ok(()) } }\n",
        )
        .unwrap();
        let registry = tempfile::tempdir().unwrap();
        let krate = registry.path().join("evil-1.0.0");
        write_crate(&krate, false);
        std::fs::remove_dir_all(krate.join("src")).unwrap();
        std::os::unix::fs::symlink(outside.path().join("elsewhere"), krate.join("src")).unwrap();
        assert_eq!(
            source_dir(
                tempfile::tempdir().unwrap().path(),
                &[registry.path().to_path_buf()],
                "evil",
                "1.0.0"
            ),
            None
        );
        // Read directly, the start directory is refused too.
        assert!(load_crate(&krate, "evil").is_none());
    }

    #[test]
    fn a_version_that_is_a_path_is_refused() {
        for version in ["../../x", "1.0.0/../../etc", "1\\0", "", "1.0.0/..", "a b"] {
            assert!(!is_safe_version(version), "{version}");
        }
        assert!(is_safe_version("1.0.0-beta.1+build"));
        let repo = tempfile::tempdir().unwrap();
        std::fs::write(
            repo.path().join("Cargo.lock"),
            "[[package]]\nname = \"evil\"\nversion = \"../../x\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\n",
        )
        .unwrap();
        assert_eq!(locked_version(repo.path(), "evil"), None);
        let registry = tempfile::tempdir().unwrap();
        write_crate(&registry.path().join("evil-1.0.0"), true);
        assert_eq!(
            source_dir(
                repo.path(),
                &[registry.path().to_path_buf()],
                "evil",
                "../evil-1.0.0"
            ),
            None
        );
    }

    #[test]
    fn a_crates_sources_are_read_up_to_a_byte_cap() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("src")).unwrap();
        let pad = format!("// {}\n", "x".repeat(MAX_FILE_BYTES as usize - 2000));
        for i in 0..12 {
            std::fs::write(
                dir.path().join(format!("src/m{i}.rs")),
                format!("{pad}pub fn f{i}() -> io::Result<()> {{ Ok(()) }}\n"),
            )
            .unwrap();
        }
        let index = load_crate(dir.path(), "big").unwrap();
        assert!(!index.fns.is_empty());
        assert!(index.fns.len() < 12, "{} files read", index.fns.len());
    }

    #[test]
    fn crate_names_are_plain_identifiers() {
        assert!(is_plain_name("csv-core"));
        assert!(!is_plain_name("../x"));
        assert!(!is_plain_name("a/b"));
    }
}
