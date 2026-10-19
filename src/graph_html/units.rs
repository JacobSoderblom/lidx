//! Applications and libraries ("units") found from a repository's manifests,
//! for the application view of `lidx graph`.
//!
//! A unit is what a person deploys or depends on: a .NET project, a
//! `package.json` package, a Python project, a crate, a Go module or main
//! package, a directory with a Dockerfile, or a folder of infrastructure
//! files. The innermost unit owns a file; files no manifest owns form one
//! `(loose)` unit per top-level folder. Detection reads manifests and a few
//! source lines from disk. It never runs anything, and nothing is
//! repository-specific.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::Path;

/// Manifests and scanned sources larger than this are skipped.
const MAX_MANIFEST_BYTES: u64 = 1_000_000;
/// Bytes of a source file read for an entry-point marker.
const MAX_SCAN_BYTES: usize = 64 * 1024;
/// Directories whose manifests are build output or vendored code.
const SKIP_DIRS: &[&str] = &[
    "node_modules",
    "bin",
    "obj",
    "target",
    "dist",
    "build",
    "out",
    "vendor",
    "__pycache__",
    "venv",
    "site-packages",
];
/// Indexed languages that count as code (not configuration or docs).
const CODE_LANGS: &[&str] = &[
    "python",
    "csharp",
    "typescript",
    "tsx",
    "javascript",
    "rust",
    "go",
    "lua",
    "sql",
    "postgres",
    "proto",
];

/// What a unit is for. Apps sort first; hidden kinds are embedded but
/// start hidden.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Kind {
    Ui,
    Desktop,
    Api,
    Service,
    Worker,
    Cli,
    Lib,
    Loose,
    Data,
    Infra,
    Test,
    Example,
    Docs,
    /// CI, editor and agent configuration (`.github/`, ...).
    Tooling,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Ui => "ui",
            Kind::Desktop => "desktop",
            Kind::Api => "api",
            Kind::Service => "service",
            Kind::Worker => "worker",
            Kind::Cli => "cli",
            Kind::Lib => "lib",
            Kind::Loose => "loose",
            Kind::Data => "data",
            Kind::Infra => "infra",
            Kind::Test => "test",
            Kind::Example => "example",
            Kind::Docs => "docs",
            Kind::Tooling => "tooling",
        }
    }

    pub fn is_app(self) -> bool {
        matches!(
            self,
            Kind::Ui | Kind::Desktop | Kind::Api | Kind::Service | Kind::Worker | Kind::Cli
        )
    }

    pub fn hidden(self) -> bool {
        matches!(
            self,
            Kind::Test | Kind::Example | Kind::Docs | Kind::Tooling
        )
    }
}

/// Package ecosystems, so a name only matches a name of its own kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Eco {
    Npm,
    Py,
    Cargo,
    Go,
    Nuget,
}

/// Where a manifest says something: the line, for the panel and tooltips.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Evidence {
    pub file: String,
    pub line: i64,
    pub text: String,
}

/// A declared dependency before it is resolved to a unit.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DepRef {
    /// A package name another manifest in the repository may declare.
    Name(Eco, String),
    /// A repo-relative directory or manifest file.
    Path(String),
}

#[derive(Clone, Debug)]
pub struct Unit {
    pub name: String,
    pub kind: Kind,
    /// Directory the unit owns (trailing `/`; empty for the repo root).
    pub root: String,
    /// Single files owned by an infra unit made of loose files.
    pub file_roots: Vec<String>,
    pub manifests: Vec<String>,
    /// Names other manifests may depend on it by.
    pub names: BTreeSet<(Eco, String)>,
    /// Normalised names an image or a deployment may call it.
    pub aliases: BTreeSet<String>,
    pub deps: Vec<(DepRef, Evidence)>,
    /// External runtime packages its manifests declare.
    pub packages: BTreeSet<String>,
    /// Commands it installs or entry points it declares.
    pub entries: BTreeSet<String>,
    /// Infra: image names (normalised) and build directories (`dir:<path>`)
    /// it deploys.
    pub images: Vec<(String, Evidence)>,
    /// Why it has its kind, in a few words ("Sdk.Web", "start script").
    pub why: String,
    /// Dockerfiles that build it.
    pub dockerfiles: Vec<String>,
}

impl Unit {
    fn new(name: &str, kind: Kind, root: &str, why: &str) -> Self {
        let mut aliases = BTreeSet::new();
        aliases.insert(norm(name));
        if let Some(last) = name.rsplit(['.', '/']).next() {
            aliases.insert(norm(last));
        }
        if let Some(base) = root.trim_end_matches('/').rsplit('/').next()
            && !base.is_empty()
        {
            aliases.insert(norm(base));
        }
        aliases.remove("");
        Unit {
            name: name.to_string(),
            kind,
            root: root.to_string(),
            file_roots: Vec::new(),
            manifests: Vec::new(),
            names: BTreeSet::new(),
            aliases,
            deps: Vec::new(),
            packages: BTreeSet::new(),
            entries: BTreeSet::new(),
            images: Vec::new(),
            why: why.to_string(),
            dockerfiles: Vec::new(),
        }
    }

    /// The root shown to people: `./` for the repo root.
    pub fn display_root(&self) -> String {
        if self.root.is_empty() && !self.file_roots.is_empty() {
            return self.file_roots.join(", ");
        }
        if self.root.is_empty() {
            "./".to_string()
        } else {
            self.root.clone()
        }
    }
}

/// Lowercase letters and digits only: `Foo.Api`, `foo-api` and `foo_api`
/// all become `fooapi`.
pub fn norm(s: &str) -> String {
    s.chars()
        .filter(char::is_ascii_alphanumeric)
        .map(|c| c.to_ascii_lowercase())
        .collect()
}

/// Which unit owns a file or directory: the innermost root above it.
#[derive(Default, Debug)]
pub struct Owners {
    by_root: HashMap<String, usize>,
    by_file: HashMap<String, usize>,
}

impl Owners {
    pub fn new(units: &[Unit]) -> Self {
        let mut o = Owners::default();
        for (i, u) in units.iter().enumerate() {
            if u.file_roots.is_empty() {
                o.by_root.entry(u.root.clone()).or_insert(i);
            }
            for f in &u.file_roots {
                o.by_file.entry(f.clone()).or_insert(i);
            }
        }
        o
    }

    /// The unit owning `path` (a file, or a directory with a trailing `/`).
    pub fn owner(&self, path: &str) -> Option<usize> {
        if let Some(&u) = self.by_file.get(path) {
            return Some(u);
        }
        if path.ends_with('/')
            && let Some(&u) = self.by_root.get(path)
        {
            return Some(u);
        }
        let trimmed = path.trim_end_matches('/');
        for (i, _) in trimmed.rmatch_indices('/') {
            if let Some(&u) = self.by_root.get(&trimmed[..=i]) {
                return Some(u);
            }
        }
        self.by_root.get("").copied()
    }
}

/// `dir` of a repo-relative file, with a trailing `/` (empty at the root).
fn dir_of(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[..=i],
        None => "",
    }
}

fn file_name(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

fn stem(path: &str) -> &str {
    let name = file_name(path);
    name.rsplit_once('.').map(|(s, _)| s).unwrap_or(name)
}

/// Every ancestor directory of `path` (with trailing `/`), and the root.
fn ancestors(path: &str) -> impl Iterator<Item = &str> {
    let trimmed = path.trim_end_matches('/');
    let own = path.ends_with('/').then_some(path);
    own.into_iter()
        .chain(
            trimmed
                .rmatch_indices('/')
                .map(move |(i, _)| &trimmed[..=i]),
        )
        .chain(std::iter::once(""))
}

/// `base` (a directory with a trailing `/`) joined with a relative path that
/// may use `..` and backslashes; `None` when it leaves the repository.
fn join_rel(base: &str, rel: &str) -> Option<String> {
    let mut parts: Vec<&str> = base.split('/').filter(|s| !s.is_empty()).collect();
    let rel = rel.replace('\\', "/");
    for seg in rel.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                parts.pop()?;
            }
            s => parts.push(s),
        }
    }
    let mut out = parts.join("/");
    if out.is_empty() {
        return Some(String::new());
    }
    if rel.ends_with('/') {
        out.push('/');
    }
    Some(out)
}

/// Word tokens of a name or path: `Shop.Tests.Common` -> shop, tests, common;
/// `fooBarTests` -> foo, bar, tests.
fn tokens(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut prev_lower = false;
    for c in s.chars() {
        if !c.is_alphanumeric() {
            if !cur.is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            prev_lower = false;
            continue;
        }
        if c.is_uppercase() && prev_lower && !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
        prev_lower = c.is_lowercase() || c.is_ascii_digit();
        cur.extend(c.to_lowercase());
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// Directories of test inputs: the manifests, Dockerfiles and main packages
/// in them describe fixtures, not units of this repository.
const FIXTURE_DIRS: &[&str] = &[
    "testdata",
    "test-fixtures",
    "test_fixtures",
    "fixtures",
    "fixture",
    "__fixtures__",
    "__snapshots__",
];
/// Whether `path` lies in a fixture or hidden (`.make/`) directory.
fn in_fixture(path: &str) -> bool {
    let mut segs: Vec<&str> = path.split('/').collect();
    segs.pop();
    segs.iter().any(|s| {
        FIXTURE_DIRS.contains(s) || s.contains("testdata") || (s.starts_with('.') && s.len() > 1)
    })
}
const BENCH_WORDS: &[&str] = &["benchmark", "benchmarks", "bench", "benches", "perf"];
/// A command named for one of these, or kept in a folder named for one of
/// `TOOL_DIRS`, is a developer tool, not a product.
const TOOL_WORDS: &[&str] = &["generate", "generator", "gen", "codegen"];
const TOOL_DIRS: &[&str] = &["tools", "scripts", "hack"];
const TEST_WORDS: &[&str] = &[
    "test", "tests", "testing", "spec", "specs", "e2e", "testdata", "fixtures", "fixture",
];
const EXAMPLE_WORDS: &[&str] = &["example", "examples", "sample", "samples", "demo", "demos"];
const DOCS_WORDS: &[&str] = &["docs", "doc", "documentation"];

fn has_word(words: &[String], set: &[&str]) -> bool {
    words.iter().any(|w| set.contains(&w.as_str()))
}

/// `(1-based line, line text)` of the byte offset `at` in `text`.
fn line_at(text: &str, at: usize) -> (i64, String) {
    let at = at.min(text.len());
    let line = text[..at].matches('\n').count() as i64 + 1;
    let start = text[..at].rfind('\n').map(|i| i + 1).unwrap_or(0);
    let end = text[at..].find('\n').map(|i| at + i).unwrap_or(text.len());
    (line, text[start..end].trim().to_string())
}

/// 1-based line of the first line that mentions `needle`, else 1.
fn line_of(text: &str, needle: &str) -> (i64, String) {
    match text.find(needle) {
        Some(at) => line_at(text, at),
        None => (1, String::new()),
    }
}

fn read_text(abs: &Path, cap: u64) -> Option<String> {
    let meta = std::fs::metadata(abs).ok()?;
    if meta.len() > cap {
        return None;
    }
    std::fs::read_to_string(abs).ok()
}

/// The first `MAX_SCAN_BYTES` of a source file.
fn read_head(abs: &Path) -> Option<String> {
    use std::io::Read;
    let mut f = std::fs::File::open(abs).ok()?;
    let mut buf = vec![0; MAX_SCAN_BYTES];
    let n = f.read(&mut buf).ok()?;
    buf.truncate(n);
    Some(String::from_utf8_lossy(&buf).into_owned())
}

/// The value of `attr="..."` inside the tag text `tag`.
fn xml_attr<'a>(tag: &'a str, attr: &str) -> Option<&'a str> {
    let lower = tag.to_ascii_lowercase();
    let key = format!("{}=", attr.to_ascii_lowercase());
    let mut from = 0;
    while let Some(i) = lower[from..].find(&key) {
        let at = from + i;
        // A whole attribute name: preceded by whitespace.
        if at > 0 && !lower.as_bytes()[at - 1].is_ascii_whitespace() {
            from = at + key.len();
            continue;
        }
        let rest = &tag[at + key.len()..];
        let quote = rest.chars().next()?;
        if quote != '"' && quote != '\'' {
            return None;
        }
        let end = rest[1..].find(quote)?;
        return Some(&rest[1..1 + end]);
    }
    None
}

/// Every `<Element ...>` start tag in `xml`, with its byte offset.
fn xml_tags<'a>(xml: &'a str, element: &str) -> Vec<(usize, &'a str)> {
    let lower = xml.to_ascii_lowercase();
    let open = format!("<{}", element.to_ascii_lowercase());
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(i) = lower[from..].find(&open) {
        let at = from + i;
        let after = lower
            .as_bytes()
            .get(at + open.len())
            .copied()
            .unwrap_or(b'>');
        from = at + open.len();
        if !(after.is_ascii_whitespace() || after == b'>' || after == b'/') {
            continue;
        }
        let end = xml[at..].find('>').map(|e| at + e + 1).unwrap_or(xml.len());
        out.push((at, &xml[at..end]));
        from = end;
    }
    out
}

/// Text of the first `<element>text</element>`.
fn xml_text<'a>(xml: &'a str, element: &str) -> Option<&'a str> {
    let lower = xml.to_ascii_lowercase();
    let open = format!("<{}>", element.to_ascii_lowercase());
    let close = format!("</{}>", element.to_ascii_lowercase());
    let start = lower.find(&open)? + open.len();
    let end = start + lower[start..].find(&close)?;
    Some(xml[start..end].trim())
}

/// NuGet packages that are build or test tooling, not something the
/// project uses at run time.
fn nuget_tooling(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    l.ends_with(".analyzers")
        || l.starts_with("coverlet")
        || l.starts_with("stylecop")
        || l.starts_with("microsoft.codeanalysis")
        || l == "microsoft.net.test.sdk"
        || l.starts_with("xunit")
        || l.starts_with("nunit")
        || l.starts_with("mstest")
        || l.starts_with("microsoft.visualstudio")
}

fn is_dockerfile(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    l == "dockerfile"
        || l.starts_with("dockerfile.")
        || l.ends_with(".dockerfile")
        || l == "containerfile"
}

/// One repository's files, as the detector sees them.
struct Repo<'a> {
    root: &'a Path,
    /// Every non-ignored file on disk, repo-relative.
    disk: HashSet<String>,
    /// Indexed files and their languages.
    indexed: &'a [(String, String)],
    /// Directories (trailing `/`, and the root) with an indexed file below.
    indexed_dirs: HashSet<String>,
    /// Directories with an indexed code file below.
    code_dirs: HashSet<String>,
    /// Directories with a `src/bin/` file (crates with binaries).
    bin_dirs: HashSet<String>,
}

impl Repo<'_> {
    fn abs(&self, rel: &str) -> std::path::PathBuf {
        self.root.join(rel)
    }
}

/// Every file under `root` the indexer's ignore rules keep, skipping
/// hidden and build-output directories.
fn walk(root: &Path) -> HashSet<String> {
    let mut out = HashSet::new();
    let walker = ignore::WalkBuilder::new(root)
        .hidden(false)
        .git_ignore(true)
        .git_global(true)
        .git_exclude(true)
        .ignore(true)
        .parents(true)
        .require_git(false)
        .filter_entry(|e| {
            let name = e.file_name().to_string_lossy();
            e.depth() == 0
                || !(name.starts_with('.')
                    || (e.file_type().is_some_and(|t| t.is_dir())
                        && SKIP_DIRS.contains(&name.as_ref())))
        })
        .build();
    for entry in walker.flatten() {
        if !entry.file_type().is_some_and(|t| t.is_file()) {
            continue;
        }
        if let Ok(rel) = entry.path().strip_prefix(root) {
            out.insert(crate::util::normalize_path(rel));
        }
    }
    out
}

/// Units of the repository at `root`, given its indexed `(path, language)`
/// files. Every indexed file has exactly one owner among the result (see
/// [`Owners`]); units that own no indexed file are dropped, except infra.
pub fn detect(root: &Path, indexed: &[(String, String)]) -> Vec<Unit> {
    detect_with_disk(root, walk(root), indexed)
}

fn detect_with_disk(root: &Path, disk: HashSet<String>, indexed: &[(String, String)]) -> Vec<Unit> {
    let mut indexed_dirs = HashSet::new();
    let mut code_dirs = HashSet::new();
    for (path, lang) in indexed {
        let code = CODE_LANGS.contains(&lang.as_str());
        for d in ancestors(dir_of(path)) {
            indexed_dirs.insert(d.to_string());
            if code {
                code_dirs.insert(d.to_string());
            }
        }
    }
    // From the index too: the walk skips `bin/` folders (.NET output),
    // which holds a crate's `src/bin/`. Test servers there
    // (`src/bin/test_server.rs`) make no binary of the crate.
    let bin_dirs: HashSet<String> = disk
        .iter()
        .chain(indexed.iter().map(|(p, _)| p))
        .filter(|p| !has_word(&tokens(stem(p)), TEST_WORDS))
        .filter_map(|p| {
            let at = if p.starts_with("src/bin/") {
                0
            } else {
                p.find("/src/bin/")? + 1
            };
            Some(p[..at].to_string())
        })
        .collect();
    let repo = Repo {
        root,
        bin_dirs,
        disk,
        indexed,
        indexed_dirs,
        code_dirs,
    };

    // Manifests by directory.
    #[derive(Default)]
    struct Dir {
        dotnet: Vec<String>,
        sqlproj: Vec<String>,
        package_json: Option<String>,
        pyproject: Option<String>,
        setup_py: Option<String>,
        cargo: Option<String>,
        gomod: Option<String>,
        dockerfiles: Vec<String>,
        js_workspace_root: bool,
    }
    let mut dirs: BTreeMap<String, Dir> = BTreeMap::new();
    let mut sorted: Vec<&String> = repo.disk.iter().collect();
    sorted.sort();
    for path in sorted {
        let dir = dir_of(path);
        if !repo.indexed_dirs.contains(dir) || in_fixture(path) {
            continue;
        }
        let name = file_name(path);
        let lower = name.to_ascii_lowercase();
        let d = dirs.entry(dir.to_string()).or_default();
        if lower.ends_with(".csproj") || lower.ends_with(".fsproj") || lower.ends_with(".vbproj") {
            d.dotnet.push(path.clone());
        } else if lower.ends_with(".sqlproj") {
            d.sqlproj.push(path.clone());
        } else if name == "package.json" {
            d.package_json = Some(path.clone());
        } else if name == "pyproject.toml" {
            d.pyproject = Some(path.clone());
        } else if name == "setup.py" {
            d.setup_py = Some(path.clone());
        } else if name == "Cargo.toml" {
            d.cargo = Some(path.clone());
        } else if name == "go.mod" {
            d.gomod = Some(path.clone());
        } else if is_dockerfile(name) {
            d.dockerfiles.push(path.clone());
        } else if name == "pnpm-workspace.yaml" || name == "lerna.json" {
            d.js_workspace_root = true;
        }
    }

    let mut units: Vec<Unit> = Vec::new();
    // Go modules: (unit index, module dir).
    let mut go_modules: Vec<(usize, String)> = Vec::new();
    for (dir, d) in &dirs {
        let pkg = d
            .package_json
            .as_ref()
            .and_then(|p| read_text(&repo.abs(p), MAX_MANIFEST_BYTES).map(|t| (p, t)))
            .and_then(|(p, t)| PackageJson::parse(p, &t));
        let mut unit: Option<Unit> = None;
        for proj in &d.dotnet {
            if let Some(text) = read_text(&repo.abs(proj), MAX_MANIFEST_BYTES) {
                let u = dotnet_unit(&repo, proj, &text);
                if unit.is_none() {
                    unit = Some(u);
                } else {
                    // A second project in one folder: its own unit.
                    units.push(u);
                }
            }
        }
        if unit.is_none()
            && let Some(proj) = d.sqlproj.first()
        {
            let mut u = Unit::new(stem(proj), Kind::Data, dir, "SQL project");
            u.manifests.push(proj.clone());
            unit = Some(u);
        }
        if unit.is_none()
            && let Some(p) = &d.pyproject
            && let Some(text) = read_text(&repo.abs(p), MAX_MANIFEST_BYTES)
        {
            unit = pyproject_unit(dir, p, &text);
        }
        if unit.is_none()
            && let Some(p) = &d.setup_py
            && let Some(text) = read_text(&repo.abs(p), MAX_MANIFEST_BYTES)
        {
            unit = Some(setup_py_unit(dir, p, &text));
        }
        if unit.is_none()
            && let Some(p) = &d.cargo
            && let Some(text) = read_text(&repo.abs(p), MAX_MANIFEST_BYTES)
        {
            unit = cargo_unit(&repo, dir, p, &text);
        }
        if unit.is_none()
            && let Some(p) = &d.gomod
            && let Some(text) = read_text(&repo.abs(p), MAX_MANIFEST_BYTES)
        {
            unit = Some(gomod_unit(dir, p, &text));
            go_modules.push((units.len(), dir.clone()));
        }
        match (unit, pkg) {
            (Some(mut u), Some(pkg)) => {
                // A package.json next to another manifest is its tooling
                // (scripts, a version for release tools): merge it.
                pkg.merge_into(&mut u);
                units.push(u);
            }
            (Some(u), None) => units.push(u),
            (None, Some(pkg)) => {
                // A workspace root is a unit only when it is a package itself.
                let workspace_root = (pkg.workspaces || d.js_workspace_root) && !pkg.entry;
                let has_docker = !d.dockerfiles.is_empty();
                let tauri = repo.disk.contains(&format!("{dir}src-tauri/Cargo.toml"));
                if let Some(u) = pkg.into_unit(dir, workspace_root, has_docker, tauri) {
                    units.push(u);
                }
            }
            (None, None) => {}
        }
    }

    // A Tauri app's Rust side (`src-tauri/`) belongs to the app around it.
    let mut i = 0;
    while i < units.len() {
        let parent_dir = units[i].root.strip_suffix("src-tauri/").map(str::to_string);
        if let Some(parent_dir) = parent_dir
            && let Some(p) = units.iter().position(|u| u.root == parent_dir)
            && p != i
        {
            let tauri = units.remove(i);
            let p = if p > i { p - 1 } else { p };
            let host = &mut units[p];
            host.kind = Kind::Desktop;
            host.why = "Tauri app".to_string();
            host.manifests.extend(tauri.manifests);
            host.names.extend(tauri.names);
            host.packages.extend(tauri.packages);
            host.deps.extend(tauri.deps);
            for g in &mut go_modules {
                if g.0 > i {
                    g.0 -= 1;
                }
            }
            continue;
        }
        i += 1;
    }

    // Go main packages: a module's `package main` directories are apps.
    let mut owners = Owners::new(&units);
    for (mi, mdir) in &go_modules {
        let mut mains: BTreeSet<String> = BTreeSet::new();
        for (path, lang) in repo.indexed {
            if lang != "go"
                || !path.starts_with(mdir.as_str())
                || path.ends_with("_test.go")
                || in_fixture(path)
            {
                continue;
            }
            if owners.owner(path) != Some(*mi) {
                continue;
            }
            let Some(head) = read_head(&repo.abs(path)) else {
                continue;
            };
            if head
                .lines()
                .map(str::trim)
                .any(|l| l == "package main" || l.starts_with("package main "))
            {
                mains.insert(dir_of(path).to_string());
            }
        }
        for m in mains {
            // A main package under `internal/` builds a tool for the
            // repository itself (generators, fixture managers).
            let internal = m[mdir.len()..].split('/').any(|s| s == "internal");
            if &m == mdir {
                let u = &mut units[*mi];
                u.kind = Kind::Cli;
                u.why = "main package".to_string();
            } else {
                let name = m.trim_end_matches('/').rsplit('/').next().unwrap_or("main");
                // `cmd/syft` in module `syft`: the command keeps the name.
                if units[*mi].name == name {
                    units[*mi].name = format!("{name} (module)");
                }
                let mut u = if internal {
                    Unit::new(name, Kind::Tooling, &m, "internal tool")
                } else {
                    Unit::new(name, Kind::Cli, &m, "main package")
                };
                u.deps.push((
                    DepRef::Path(mdir.clone()),
                    Evidence {
                        file: format!("{mdir}go.mod"),
                        line: 1,
                        text: format!("main package in module {}", units[*mi].name),
                    },
                ));
                units.push(u);
            }
        }
    }

    // Python services: an entry point in the unit's own sources.
    owners = Owners::new(&units);
    let mut py_files: HashMap<usize, Vec<&String>> = HashMap::new();
    for (path, lang) in repo.indexed {
        if lang == "python"
            && let Some(o) = owners.owner(path)
        {
            py_files.entry(o).or_default().push(path);
        }
    }
    for (ui, files) in py_files {
        let u = &units[ui];
        if !u.names.iter().any(|(e, _)| *e == Eco::Py) || !matches!(u.kind, Kind::Lib | Kind::Cli) {
            continue;
        }
        let mut found: Option<(Kind, &str)> = None;
        for f in files {
            let words = tokens(f);
            if has_word(&words, TEST_WORDS) {
                continue;
            }
            let name = file_name(f);
            if name == "manage.py" {
                found = Some((Kind::Api, "Django app"));
                break;
            }
            let Some(head) = read_head(&repo.abs(f)) else {
                continue;
            };
            for (marker, kind, why) in [
                ("FastAPI(", Kind::Api, "FastAPI app"),
                ("Flask(__name__", Kind::Api, "Flask app"),
                ("get_asgi_application(", Kind::Api, "Django app"),
                ("get_wsgi_application(", Kind::Api, "Django app"),
                ("uvicorn.run(", Kind::Api, "uvicorn entry point"),
                ("grpc.server(", Kind::Service, "gRPC server"),
                ("grpc.aio.server(", Kind::Service, "gRPC server"),
            ] {
                if head.contains(marker) {
                    found = Some((kind, why));
                    break;
                }
            }
            if found.is_some() {
                break;
            }
            if name == "__main__.py" && found.is_none() && units[ui].kind == Kind::Lib {
                found = Some((Kind::Cli, "__main__ module"));
            }
        }
        if let Some((kind, why)) = found {
            let u = &mut units[ui];
            if u.kind == Kind::Lib || kind != Kind::Cli {
                u.kind = kind;
                u.why = why.to_string();
            }
        }
    }

    // Dockerfiles make what they build deployable.
    owners = Owners::new(&units);
    for (dir, d) in &dirs {
        for df in &d.dockerfiles {
            let text = read_text(&repo.abs(df), MAX_MANIFEST_BYTES).unwrap_or_default();
            // The command an ENTRYPOINT or CMD runs, when a unit inside the
            // owner is named for it (`ENTRYPOINT ["/syft"]` and `cmd/syft/`).
            let runs: Vec<String> = text
                .lines()
                .map(str::trim)
                .filter(|l| {
                    let u = l.to_ascii_uppercase();
                    u.starts_with("ENTRYPOINT") || u.starts_with("CMD")
                })
                .flat_map(|l| tokens(l).into_iter())
                .collect();
            let inner = units.iter().position(|u| {
                u.root.starts_with(dir.as_str())
                    && u.root != *dir
                    && u.kind.is_app()
                    && runs.contains(&u.name.to_lowercase())
            });
            let target = match owners.owner(dir) {
                Some(_) if inner.is_some() => inner,
                Some(o) => Some(o),
                None => {
                    let below: Vec<usize> = units
                        .iter()
                        .enumerate()
                        .filter(|(_, u)| u.root.starts_with(dir.as_str()) && u.root != *dir)
                        .map(|(i, _)| i)
                        .collect();
                    let named: Vec<usize> = below
                        .iter()
                        .copied()
                        .filter(|&i| {
                            let u = &units[i];
                            let rel = &u.root[dir.len()..];
                            (!rel.is_empty() && text.contains(rel.trim_end_matches('/')))
                                || u.manifests.iter().any(|m| text.contains(file_name(m)))
                        })
                        .collect();
                    match (named.as_slice(), below.as_slice()) {
                        ([one], _) | ([], [one]) => Some(*one),
                        _ => None,
                    }
                }
            };
            match target {
                Some(t) => {
                    let u = &mut units[t];
                    if matches!(u.kind, Kind::Lib | Kind::Loose) {
                        u.kind = Kind::Service;
                        u.why = "Dockerfile".to_string();
                    }
                    u.dockerfiles.push(df.clone());
                    if let Some(base) = dir.trim_end_matches('/').rsplit('/').next() {
                        let a = norm(base);
                        if !a.is_empty() {
                            u.aliases.insert(a);
                        }
                    }
                }
                None if repo.code_dirs.contains(dir.as_str()) && !dir.is_empty() => {
                    // A Dockerfile alone marks its directory deployable.
                    let name = dir.trim_end_matches('/').rsplit('/').next().unwrap_or(dir);
                    let mut u = Unit::new(name, Kind::Service, dir, "Dockerfile");
                    u.dockerfiles.push(df.clone());
                    units.push(u);
                    owners = Owners::new(&units);
                }
                None => {}
            }
        }
    }

    // Infrastructure: k8s, Helm, compose, Bicep and Terraform files.
    owners = Owners::new(&units);
    let mut infra_files: Vec<(String, String)> = Vec::new(); // (path, text)
    let mut infra_paths: Vec<&String> =
        repo.disk.iter().filter(|p| is_infra_candidate(p)).collect();
    infra_paths.sort();
    for p in infra_paths {
        let Some(text) = read_text(&repo.abs(p), MAX_MANIFEST_BYTES) else {
            continue;
        };
        if is_infra(p, &text) {
            infra_files.push((p.clone(), text));
        }
    }
    let unit_roots: HashSet<String> = units
        .iter()
        .flat_map(|u| ancestors(&u.root).map(str::to_string).collect::<Vec<_>>())
        .collect();
    let clean = |d: &str| !repo.code_dirs.contains(d) && !unit_roots.contains(d);
    // infra root -> files (a root ending in `/` is a directory; otherwise
    // the files sit next to code and form a unit of their own).
    let mut groups: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    infra_files.retain(|(p, _)| !in_fixture(p));
    for (fi, (path, _)) in infra_files.iter().enumerate() {
        let dir = dir_of(path);
        let owner_root = owners
            .owner(path)
            .map(|o| units[o].root.clone())
            .unwrap_or_default();
        if !clean(dir) || dir.is_empty() {
            groups.entry(format!("files:{dir}")).or_default().push(fi);
            continue;
        }
        // Climb while the parent is still free of code and below the owner.
        let mut top = dir.to_string();
        loop {
            let parent = dir_of(top.trim_end_matches('/')).to_string();
            if parent.is_empty() || parent.len() <= owner_root.len() || !clean(&parent) {
                break;
            }
            top = parent;
        }
        groups.entry(top).or_default().push(fi);
    }
    // A folder of infra with several infra subfolders splits into them.
    let mut split: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (top, files) in groups {
        if top.starts_with("files:") {
            split.insert(top, files);
            continue;
        }
        let mut children: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        for &fi in &files {
            let rest = &infra_files[fi].0[top.len()..];
            let key = match rest.find('/') {
                Some(i) => format!("{top}{}", &rest[..=i]),
                None => top.clone(),
            };
            children.entry(key).or_default().push(fi);
        }
        if children.keys().filter(|k| **k != top).count() >= 2 {
            split.extend(children);
        } else {
            split.insert(top, files);
        }
    }
    for (root, files) in split {
        let mut u = if let Some(dir) = root.strip_prefix("files:") {
            let first = &infra_files[files[0]].0;
            let name = if files.len() == 1 {
                stem(first).to_string()
            } else if dir.is_empty() {
                "deployment".to_string()
            } else {
                format!("{} deployment", dir.trim_end_matches('/'))
            };
            let mut u = Unit::new(&name, Kind::Infra, "", "deployment files");
            u.aliases.clear();
            u.file_roots = files.iter().map(|&f| infra_files[f].0.clone()).collect();
            u
        } else {
            let name = root.trim_end_matches('/').to_string();
            let mut u = Unit::new(&name, Kind::Infra, &root, "deployment files");
            u.aliases.clear();
            u
        };
        let mut kinds: BTreeSet<&str> = BTreeSet::new();
        for &fi in &files {
            let (path, text) = &infra_files[fi];
            kinds.insert(infra_kind(path));
            collect_images(path, text, &mut u.images);
        }
        u.why = kinds.into_iter().collect::<Vec<_>>().join(", ");
        units.push(u);
    }

    // Files no manifest owns: one loose unit per top-level folder.
    owners = Owners::new(&units);
    let mut loose: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for (path, lang) in repo.indexed {
        if owners.owner(path).is_none() {
            let top = match path.find('/') {
                Some(i) => path[..=i].to_string(),
                None => String::new(),
            };
            loose.entry(top).or_default().push(lang);
        }
    }
    for (top, langs) in loose {
        let name = if top.is_empty() {
            "./".to_string()
        } else {
            top.clone()
        };
        let kind = if top.starts_with('.') {
            Kind::Tooling
        } else if langs.iter().all(|l| *l == "sql" || *l == "postgres") {
            Kind::Data
        } else if langs.iter().all(|l| !CODE_LANGS.contains(l)) {
            Kind::Docs
        } else {
            Kind::Loose
        };
        let mut u = Unit::new(&name, kind, &top, "files outside any manifest");
        u.aliases.clear();
        units.push(u);
    }

    // Kinds the path or name implies: tests, examples, docs.
    for u in &mut units {
        if matches!(u.kind, Kind::Infra | Kind::Tooling) {
            continue;
        }
        let mut words = tokens(&u.root);
        words.extend(tokens(&u.name));
        if has_word(&words, EXAMPLE_WORDS) {
            u.kind = Kind::Example;
            u.why = "example".to_string();
        } else if has_word(&words, BENCH_WORDS)
            || (u.kind == Kind::Cli
                && (has_word(&tokens(&u.name), TOOL_WORDS)
                    || u.root
                        .trim_end_matches('/')
                        .split('/')
                        .rev()
                        .skip(1)
                        .any(|s| TOOL_DIRS.contains(&s))))
        {
            u.kind = Kind::Tooling;
            u.why = if has_word(&words, BENCH_WORDS) {
                "benchmarks".to_string()
            } else {
                "developer tool".to_string()
            };
        } else if has_word(&words, TEST_WORDS) && u.kind != Kind::Test {
            u.kind = Kind::Test;
            u.why = "tests".to_string();
        } else if has_word(&tokens(&u.root), DOCS_WORDS) && !u.kind.is_app() {
            u.kind = Kind::Docs;
            u.why = "docs".to_string();
        }
    }

    // Keep units that own something.
    owners = Owners::new(&units);
    let mut owned = vec![0usize; units.len()];
    for (path, _) in repo.indexed {
        if let Some(o) = owners.owner(path) {
            owned[o] += 1;
        }
    }
    let mut keep: Vec<Unit> = units
        .into_iter()
        .zip(owned)
        .filter(|(u, n)| *n > 0 || u.kind == Kind::Infra)
        .map(|(u, _)| u)
        .collect();
    // Unique names.
    let mut seen: HashMap<String, usize> = HashMap::new();
    for u in &keep {
        *seen.entry(u.name.clone()).or_default() += 1;
    }
    for u in &mut keep {
        if seen[&u.name] > 1 {
            u.name = format!("{} ({})", u.name, u.display_root());
        }
    }
    // A command-line package other units depend on is a library that
    // also installs a command.
    let depended: HashSet<usize> = resolve_deps(&keep).into_iter().map(|(_, t, _)| t).collect();
    for (i, u) in keep.iter_mut().enumerate() {
        if u.kind == Kind::Cli && depended.contains(&i) && u.dockerfiles.is_empty() {
            u.kind = Kind::Lib;
            u.why = "library with a command".to_string();
        }
    }
    keep.sort_by(|a, b| (a.kind, &a.name).cmp(&(b.kind, &b.name)));
    keep
}

/// A .NET project: an app by SDK, output type or a `Program.cs`.
fn dotnet_unit(repo: &Repo, proj: &str, text: &str) -> Unit {
    let dir = dir_of(proj);
    let name = stem(proj);
    let sdk = xml_tags(text, "Project")
        .first()
        .and_then(|(_, t)| xml_attr(t, "Sdk"))
        .unwrap_or("")
        .to_string();
    let output = xml_text(text, "OutputType")
        .unwrap_or("")
        .to_ascii_lowercase();
    let mut packages = BTreeSet::new();
    let mut test_pkg = false;
    let mut hosting = false;
    for (_, tag) in xml_tags(text, "PackageReference") {
        let Some(inc) = xml_attr(tag, "Include") else {
            continue;
        };
        let l = inc.to_ascii_lowercase();
        test_pkg |= l.starts_with("xunit")
            || l.starts_with("nunit")
            || l.starts_with("mstest")
            || l == "microsoft.net.test.sdk";
        hosting |= l.starts_with("microsoft.extensions.hosting");
        if !nuget_tooling(inc) {
            packages.insert(inc.to_string());
        }
    }
    let program = repo.disk.contains(&format!("{dir}Program.cs"))
        || repo.disk.contains(&format!("{dir}Program.fs"));
    let flag = |el: &str| xml_text(text, el).is_some_and(|v| v.eq_ignore_ascii_case("true"));
    let words = tokens(name);
    let (kind, why) = if test_pkg || has_word(&words, TEST_WORDS) {
        (Kind::Test, "test project".to_string())
    } else if sdk.ends_with(".Web") || sdk.contains(".Web.") {
        (Kind::Api, sdk.clone())
    } else if sdk.ends_with(".Worker") {
        (Kind::Worker, sdk.clone())
    } else if sdk.contains("BlazorWebAssembly") {
        (Kind::Ui, sdk.clone())
    } else if output == "winexe" || flag("UseWPF") || flag("UseWindowsForms") || flag("UseMaui") {
        (Kind::Desktop, "desktop app".to_string())
    } else if output == "exe" || program {
        if hosting {
            (Kind::Worker, "hosted service".to_string())
        } else {
            (Kind::Cli, "console app".to_string())
        }
    } else {
        (Kind::Lib, "class library".to_string())
    };
    let mut u = Unit::new(name, kind, dir, &why);
    u.manifests.push(proj.to_string());
    u.names.insert((Eco::Nuget, norm(name)));
    if let Some(asm) = xml_text(text, "AssemblyName") {
        u.names.insert((Eco::Nuget, norm(asm)));
        u.aliases.insert(norm(asm));
    }
    u.packages = packages;
    for (at, tag) in xml_tags(text, "ProjectReference") {
        if let Some(inc) = xml_attr(tag, "Include")
            && let Some(path) = join_rel(dir, inc)
        {
            let (line, snippet) = line_at(text, at);
            u.deps.push((
                DepRef::Path(path),
                Evidence {
                    file: proj.to_string(),
                    line,
                    text: snippet,
                },
            ));
        }
    }
    u
}

/// The fields of a `package.json` that decide what it is.
struct PackageJson {
    path: String,
    text: String,
    name: Option<String>,
    /// Script name -> command.
    scripts: BTreeMap<String, String>,
    deps: BTreeSet<String>,
    dev_deps: BTreeSet<String>,
    /// Dependencies by spec (`file:`, `link:`, `workspace:`).
    specs: BTreeMap<String, String>,
    bin: Vec<String>,
    entry: bool,
    workspaces: bool,
}

impl PackageJson {
    fn parse(path: &str, text: &str) -> Option<Self> {
        let v: serde_json::Value = serde_json::from_str(text).ok()?;
        let keys = |k: &str| -> BTreeMap<String, String> {
            v.get(k)
                .and_then(|o| o.as_object())
                .map(|o| {
                    o.iter()
                        .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                        .collect()
                })
                .unwrap_or_default()
        };
        let deps = keys("dependencies");
        let mut specs = BTreeMap::new();
        let mut dev = keys("devDependencies");
        dev.extend(keys("peerDependencies"));
        dev.extend(keys("optionalDependencies"));
        for (k, s) in deps.iter().chain(dev.iter()) {
            specs.insert(k.clone(), s.clone());
        }
        let name = v.get("name").and_then(|n| n.as_str()).map(str::to_string);
        let bin = match v.get("bin") {
            Some(serde_json::Value::String(_)) => vec![name.clone().unwrap_or_default()],
            Some(serde_json::Value::Object(o)) => o.keys().cloned().collect(),
            _ => Vec::new(),
        };
        Some(PackageJson {
            path: path.to_string(),
            text: text.to_string(),
            name,
            scripts: keys("scripts"),
            deps: deps.into_keys().collect(),
            dev_deps: dev.into_keys().collect(),
            specs,
            bin,
            entry: ["main", "module", "exports", "types", "typings"]
                .iter()
                .any(|k| v.get(k).is_some()),
            workspaces: v.get("workspaces").is_some(),
        })
    }

    fn any_dep(&self, names: &[&str]) -> Option<String> {
        self.deps
            .iter()
            .chain(self.dev_deps.iter())
            .find(|d| {
                names
                    .iter()
                    .any(|n| d == n || (n.ends_with('/') && d.starts_with(n)))
            })
            .cloned()
    }

    fn add_deps(&self, u: &mut Unit, dir: &str) {
        for (name, spec) in &self.specs {
            let (line, text) = line_of(&self.text, &format!("\"{name}\""));
            let ev = Evidence {
                file: self.path.clone(),
                line,
                text,
            };
            let path = ["file:", "link:", "portal:"]
                .iter()
                .find_map(|p| spec.strip_prefix(p));
            match path {
                Some(rel) => {
                    if let Some(p) = join_rel(dir, &format!("{rel}/")) {
                        u.deps.push((DepRef::Path(p), ev));
                    }
                }
                None => u.deps.push((DepRef::Name(Eco::Npm, name.clone()), ev)),
            }
        }
        for d in &self.deps {
            if !d.starts_with("@types/") {
                u.packages.insert(d.clone());
            }
        }
    }

    /// Fold a tooling `package.json` into the unit of the manifest next to it.
    fn merge_into(self, u: &mut Unit) {
        u.manifests.push(self.path.clone());
        if let Some(n) = &self.name {
            u.names.insert((Eco::Npm, n.clone()));
            let a = norm(n.rsplit('/').next().unwrap_or(n));
            if !a.is_empty() {
                u.aliases.insert(a);
            }
        }
        // Only local packages: tooling deps are not what the unit uses.
        for (name, spec) in &self.specs {
            if spec.starts_with("workspace:") || spec.starts_with("file:") {
                let (line, text) = line_of(&self.text, &format!("\"{name}\""));
                u.deps.push((
                    DepRef::Name(Eco::Npm, name.clone()),
                    Evidence {
                        file: self.path.clone(),
                        line,
                        text,
                    },
                ));
            }
        }
    }

    fn into_unit(self, dir: &str, workspace_root: bool, docker: bool, tauri: bool) -> Option<Unit> {
        // A workspace root's `dev` that runs every member (`turbo run dev`)
        // starts the members, not the root.
        let start = ["start", "dev", "serve"]
            .iter()
            .find(|s| {
                self.scripts
                    .get(**s)
                    .is_some_and(|cmd| !(workspace_root && runs_workspace(cmd)))
            })
            .copied();
        let framework_dep = self
            .deps
            .iter()
            .find(|d| FRAMEWORKS.contains(&d.as_str()))
            .cloned();
        let app_signal = start.is_some() || docker || framework_dep.is_some() || tauri;
        if workspace_root && !(start.is_some() || docker) {
            return None;
        }
        let name = self.name.clone().unwrap_or_else(|| {
            dir.trim_end_matches('/')
                .rsplit('/')
                .next()
                .unwrap_or("package")
                .to_string()
        });
        let lib_entry = self.entry && start.is_none() && !docker && !tauri;
        let (kind, why) = if !self.bin.is_empty() && !app_signal {
            (Kind::Cli, "bin".to_string())
        } else if !app_signal || lib_entry {
            (Kind::Lib, "package".to_string())
        } else if tauri
            || self
                .any_dep(&["@tauri-apps/api", "@tauri-apps/cli"])
                .is_some()
        {
            (Kind::Desktop, "Tauri app".to_string())
        } else if self.any_dep(&["electron"]).is_some() {
            (Kind::Desktop, "Electron app".to_string())
        } else if let Some(d) = self.any_dep(&["@docusaurus/core", "vitepress", "nextra"]) {
            (Kind::Docs, d)
        } else if let Some(d) = self.any_dep(UI_FRAMEWORKS) {
            (Kind::Ui, d)
        } else if let Some(d) = self.any_dep(API_FRAMEWORKS) {
            (Kind::Api, d)
        } else if !self.bin.is_empty() {
            (Kind::Cli, "bin".to_string())
        } else {
            (
                Kind::Service,
                start
                    .map(|s| format!("{s} script"))
                    .unwrap_or_else(|| "Dockerfile".to_string()),
            )
        };
        let short = name.rsplit('/').next().unwrap_or(&name).to_string();
        let mut u = Unit::new(&short, kind, dir, &why);
        u.name = name.clone();
        u.manifests.push(self.path.clone());
        u.names.insert((Eco::Npm, name));
        u.entries.extend(self.bin.iter().cloned());
        self.add_deps(&mut u, dir);
        Some(u)
    }
}

/// Whether a script command runs the scripts of a workspace's members
/// (`turbo run dev`, `pnpm -r dev`, `npm run dev --workspaces`).
fn runs_workspace(cmd: &str) -> bool {
    const RUNNERS: &[&str] = &[
        "turbo",
        "lerna",
        "nx",
        "rush",
        "wireit",
        "npm-run-all",
        "run-p",
        "run-s",
        "concurrently",
    ];
    const FLAGS: &[&str] = &[
        "-r",
        "--recursive",
        "--filter",
        "-f",
        "-w",
        "-ws",
        "--workspace",
        "--workspaces",
        "workspace",
        "workspaces",
    ];
    let words: Vec<String> = cmd
        .split(|c: char| c.is_whitespace() || matches!(c, '&' | ';' | '|'))
        .filter(|w| !w.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    words.iter().enumerate().any(|(i, w)| {
        RUNNERS.contains(&w.as_str())
            || (matches!(w.as_str(), "npm" | "pnpm" | "yarn")
                && words[i + 1..].iter().any(|f| {
                    FLAGS.contains(&f.as_str())
                        || f.starts_with("--filter=")
                        || f.starts_with("--workspace=")
                }))
    })
}

const FRAMEWORKS: &[&str] = &[
    "next",
    "vite",
    "react-scripts",
    "express",
    "fastify",
    "@nestjs/core",
    "electron",
    "@tauri-apps/api",
    "nuxt",
    "@sveltejs/kit",
    "@angular/core",
    "astro",
    "@remix-run/node",
    "koa",
    "@hono/node-server",
];
const UI_FRAMEWORKS: &[&str] = &[
    "next",
    "vite",
    "react-scripts",
    "react",
    "react-dom",
    "vue",
    "svelte",
    "@sveltejs/kit",
    "@angular/core",
    "nuxt",
    "astro",
    "@remix-run/react",
    "preact",
    "solid-js",
    "expo",
    "react-native",
];
const API_FRAMEWORKS: &[&str] = &[
    "express",
    "fastify",
    "@nestjs/core",
    "koa",
    "hono",
    "@hono/node-server",
    "@apollo/server",
    "restify",
];

/// A PEP 508 requirement's distribution name, normalised (PEP 503).
fn pep508_name(req: &str) -> String {
    let name: String = req
        .trim()
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        .collect();
    py_norm(&name)
}

fn py_norm(name: &str) -> String {
    let mut out = String::new();
    for c in name.chars() {
        if matches!(c, '-' | '_' | '.') {
            if !out.ends_with('-') {
                out.push('-');
            }
        } else {
            out.push(c.to_ascii_lowercase());
        }
    }
    out
}

fn pyproject_unit(dir: &str, path: &str, text: &str) -> Option<Unit> {
    let table: toml::Table = text.parse().ok()?;
    let project = table.get("project").and_then(|v| v.as_table());
    let poetry = table
        .get("tool")
        .and_then(|t| t.get("poetry"))
        .and_then(|v| v.as_table());
    let name = project
        .and_then(|p| p.get("name"))
        .or_else(|| poetry.and_then(|p| p.get("name")))
        .and_then(|v| v.as_str())?
        .to_string();
    let mut scripts: BTreeSet<String> = BTreeSet::new();
    for t in [
        project.and_then(|p| p.get("scripts")),
        project.and_then(|p| p.get("gui-scripts")),
        poetry.and_then(|p| p.get("scripts")),
    ]
    .into_iter()
    .flatten()
    .filter_map(|v| v.as_table())
    {
        scripts.extend(t.keys().cloned());
    }
    let (kind, why) = if scripts.is_empty() {
        (Kind::Lib, "package".to_string())
    } else {
        (Kind::Cli, "project.scripts".to_string())
    };
    let mut u = Unit::new(&name, kind, dir, &why);
    u.manifests.push(path.to_string());
    u.names.insert((Eco::Py, py_norm(&name)));
    u.entries = scripts;
    let ev = |needle: &str| {
        let (line, t) = line_of(text, needle);
        Evidence {
            file: path.to_string(),
            line,
            text: t,
        }
    };
    let mut path_deps: BTreeSet<String> = BTreeSet::new();
    // uv sources: `name = { path = "../lib" }` or `{ workspace = true }`.
    if let Some(src) = table
        .get("tool")
        .and_then(|t| t.get("uv"))
        .and_then(|t| t.get("sources"))
        .and_then(|v| v.as_table())
    {
        for (dep, spec) in src {
            if let Some(p) = spec.get("path").and_then(|v| v.as_str())
                && let Some(full) = join_rel(dir, &format!("{p}/"))
            {
                path_deps.insert(py_norm(dep));
                u.deps.push((DepRef::Path(full), ev(&format!("{dep} "))));
            }
        }
    }
    if let Some(deps) = project
        .and_then(|p| p.get("dependencies"))
        .and_then(|v| v.as_array())
    {
        for req in deps.iter().filter_map(|v| v.as_str()) {
            let n = pep508_name(req);
            if n.is_empty() || path_deps.contains(&n) {
                continue;
            }
            u.packages.insert(n.clone());
            u.deps.push((DepRef::Name(Eco::Py, n), ev(req)));
        }
    }
    if let Some(deps) = poetry
        .and_then(|p| p.get("dependencies"))
        .and_then(|v| v.as_table())
    {
        for (dep, spec) in deps {
            if dep == "python" {
                continue;
            }
            if let Some(p) = spec.get("path").and_then(|v| v.as_str())
                && let Some(full) = join_rel(dir, &format!("{p}/"))
            {
                u.deps.push((DepRef::Path(full), ev(&format!("{dep} "))));
                continue;
            }
            let n = py_norm(dep);
            u.packages.insert(n.clone());
            u.deps
                .push((DepRef::Name(Eco::Py, n), ev(&format!("{dep} "))));
        }
    }
    Some(u)
}

fn setup_py_unit(dir: &str, path: &str, text: &str) -> Unit {
    // `name="x"` as a whole keyword: not the `name` in `__name__` or
    // `package_name = ...`.
    let quoted_after = |key: &str| -> Option<String> {
        text.match_indices(key).find_map(|(at, _)| {
            let before = text[..at].chars().next_back();
            if before.is_some_and(|c| c.is_alphanumeric() || c == '_') {
                return None;
            }
            let rest = text[at + key.len()..]
                .trim_start()
                .strip_prefix('=')?
                .trim_start();
            let q = rest.chars().next()?;
            if q != '"' && q != '\'' {
                return None;
            }
            let end = rest[1..].find(q)?;
            Some(rest[1..1 + end].to_string())
        })
    };
    let name = quoted_after("name").unwrap_or_else(|| {
        dir.trim_end_matches('/')
            .rsplit('/')
            .next()
            .unwrap_or("setup")
            .to_string()
    });
    let cli = text.contains("console_scripts") || text.contains("scripts=");
    let (kind, why) = if cli {
        (Kind::Cli, "console_scripts")
    } else {
        (Kind::Lib, "package")
    };
    let mut u = Unit::new(&name, kind, dir, why);
    u.manifests.push(path.to_string());
    u.names.insert((Eco::Py, py_norm(&name)));
    if let Some(at) = text.find("install_requires") {
        let rest = &text[at..];
        if let (Some(open), Some(close)) = (rest.find('['), rest.find(']')) {
            for item in rest[open + 1..close.max(open + 1)].split(',') {
                let req = item.trim().trim_matches(['"', '\'']);
                let n = pep508_name(req);
                if !n.is_empty() {
                    let (line, t) = line_of(text, req);
                    u.packages.insert(n.clone());
                    u.deps.push((
                        DepRef::Name(Eco::Py, n),
                        Evidence {
                            file: path.to_string(),
                            line,
                            text: t,
                        },
                    ));
                }
            }
        }
    }
    u
}

fn cargo_unit(repo: &Repo, dir: &str, path: &str, text: &str) -> Option<Unit> {
    let table: toml::Table = text.parse().ok()?;
    let name = table.get("package")?.get("name")?.as_str()?.to_string();
    let bins: Vec<String> = table
        .get("bin")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|b| b.get("name").and_then(|n| n.as_str()).map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    let main = repo.disk.contains(&format!("{dir}src/main.rs")) || repo.bin_dirs.contains(dir);
    let mut deps: Vec<(String, toml::Value)> = Vec::new();
    for section in ["dependencies", "build-dependencies"] {
        if let Some(t) = table.get(section).and_then(|v| v.as_table()) {
            deps.extend(t.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
    }
    let dev: Vec<(String, toml::Value)> = table
        .get("dev-dependencies")
        .and_then(|v| v.as_table())
        .map(|t| t.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    let has = |n: &[&str]| deps.iter().any(|(k, _)| n.contains(&k.as_str()));
    let app = !bins.is_empty() || main;
    let (kind, why) = if has(&["tauri"]) {
        (Kind::Desktop, "Tauri app")
    } else if app && has(&["axum", "actix-web", "rocket", "warp", "poem", "tide"]) {
        (Kind::Api, "web server")
    } else if app {
        (Kind::Cli, "binary")
    } else {
        (Kind::Lib, "crate")
    };
    let mut u = Unit::new(&name, kind, dir, why);
    u.manifests.push(path.to_string());
    u.names.insert((Eco::Cargo, norm(&name)));
    u.entries.extend(bins);
    if main && u.entries.is_empty() && app {
        u.entries.insert(name.clone());
    }
    for (dep, spec) in deps.iter().chain(dev.iter()) {
        let (line, t) = line_of(text, &format!("{dep} "));
        let ev = Evidence {
            file: path.to_string(),
            line,
            text: t,
        };
        if let Some(p) = spec.get("path").and_then(|v| v.as_str())
            && let Some(full) = join_rel(dir, &format!("{p}/"))
        {
            u.deps.push((DepRef::Path(full), ev));
            continue;
        }
        let real = spec.get("package").and_then(|v| v.as_str()).unwrap_or(dep);
        u.deps.push((DepRef::Name(Eco::Cargo, norm(real)), ev));
    }
    for (dep, _) in &deps {
        u.packages.insert(dep.clone());
    }
    Some(u)
}

fn gomod_unit(dir: &str, path: &str, text: &str) -> Unit {
    let module = text
        .lines()
        .map(str::trim)
        .find_map(|l| l.strip_prefix("module "))
        .map(|m| m.trim().trim_matches('"').to_string())
        .unwrap_or_else(|| dir.trim_end_matches('/').to_string());
    let short = module.rsplit('/').next().unwrap_or(&module).to_string();
    let mut u = Unit::new(&short, Kind::Lib, dir, "Go module");
    u.manifests.push(path.to_string());
    u.names.insert((Eco::Go, module.clone()));
    let mut in_require = false;
    for (i, raw) in text.lines().enumerate() {
        let l = raw.trim();
        let ev = || Evidence {
            file: path.to_string(),
            line: i as i64 + 1,
            text: l.to_string(),
        };
        if l.starts_with("require (") {
            in_require = true;
            continue;
        }
        if in_require && l.starts_with(')') {
            in_require = false;
            continue;
        }
        let req = if in_require {
            Some(l)
        } else {
            l.strip_prefix("require ")
        };
        if let Some(req) = req {
            let m = req.split_whitespace().next().unwrap_or("");
            if m.is_empty() || m.starts_with("//") {
                continue;
            }
            u.deps.push((DepRef::Name(Eco::Go, m.to_string()), ev()));
            if !req.contains("// indirect") {
                u.packages.insert(m.to_string());
            }
        }
        if let Some(rep) = l.strip_prefix("replace ")
            && let Some((_, to)) = rep.split_once("=>")
        {
            let to = to.trim();
            if (to.starts_with("./") || to.starts_with("../"))
                && let Some(full) = join_rel(dir, &format!("{to}/"))
            {
                u.deps.push((DepRef::Path(full), ev()));
            }
        }
    }
    u
}

fn is_infra_candidate(path: &str) -> bool {
    let name = file_name(path).to_ascii_lowercase();
    name.ends_with(".bicep")
        || name.ends_with(".bicepparam")
        || name.ends_with(".tf")
        || name.ends_with(".tfvars")
        || name.ends_with(".yaml")
        || name.ends_with(".yml")
}

/// Whether a candidate file is deployment configuration: Bicep, Terraform,
/// a compose file, Helm chart files or a Kubernetes manifest.
fn is_infra(path: &str, text: &str) -> bool {
    let name = file_name(path).to_ascii_lowercase();
    if !(name.ends_with(".yaml") || name.ends_with(".yml")) {
        return true;
    }
    if name.starts_with("docker-compose") || name.starts_with("compose.") {
        return true;
    }
    if name == "chart.yaml" || name.starts_with("kustomization.") {
        return true;
    }
    // A Helm chart's templates and values.
    if path.contains("/templates/") || name.starts_with("values") {
        let mut d = dir_of(path);
        while !d.is_empty() {
            if text.contains("{{") || d.ends_with("templates/") {
                return true;
            }
            d = dir_of(d.trim_end_matches('/'));
        }
    }
    let api = text.lines().any(|l| l.starts_with("apiVersion:"));
    let kind = text.lines().any(|l| l.starts_with("kind:"));
    api && kind
}

fn infra_kind(path: &str) -> &'static str {
    let name = file_name(path).to_ascii_lowercase();
    if name.ends_with(".bicep") || name.ends_with(".bicepparam") {
        "Bicep"
    } else if name.ends_with(".tf") || name.ends_with(".tfvars") {
        "Terraform"
    } else if name.starts_with("docker-compose") || name.starts_with("compose.") {
        "compose"
    } else if name == "chart.yaml" || path.contains("/templates/") {
        "Helm"
    } else {
        "Kubernetes"
    }
}

/// Image references (`image: registry/name:tag`, Terraform
/// `image = "..."`) and compose build directories in an infra file.
fn collect_images(path: &str, text: &str, out: &mut Vec<(String, Evidence)>) {
    let dir = dir_of(path);
    for (i, raw) in text.lines().enumerate() {
        let l = raw.trim().trim_start_matches("- ").trim();
        let ev = || Evidence {
            file: path.to_string(),
            line: i as i64 + 1,
            text: l.to_string(),
        };
        let value = |key: &str| -> Option<&str> {
            let rest = l.strip_prefix(key)?.trim_start();
            let rest = rest
                .strip_prefix(':')
                .or_else(|| rest.strip_prefix('='))?
                .trim();
            let v = rest.trim_matches(['"', '\'']).trim();
            (!v.is_empty()).then_some(v)
        };
        if let Some(v) = value("image").or_else(|| value("containerImage")) {
            if let Some(n) = image_name(v) {
                out.push((n, ev()));
            }
        } else if let Some(v) = value("build").or_else(|| value("context"))
            && (v.starts_with('.') || !v.contains(':'))
            && let Some(full) = join_rel(dir, &format!("{v}/"))
        {
            out.push((format!("dir:{full}"), ev()));
        }
    }
}

/// The normalised repository name of an image reference:
/// `registry.io/team/foo-api:1.2@sha256:..` -> `fooapi`.
fn image_name(reference: &str) -> Option<String> {
    let r = reference.split('@').next().unwrap_or(reference);
    let last = r.rsplit('/').next().unwrap_or(r);
    let base = last.split(':').next().unwrap_or(last);
    // `${IMAGE}` and other templated names say nothing.
    if base.contains('$') || base.contains('{') {
        return None;
    }
    let n = norm(base);
    (!n.is_empty()).then_some(n)
}

/// A unit's language-neutral package groups: NuGet families folded
/// (`Microsoft.EntityFrameworkCore.SqlServer` -> EF Core), npm scopes
/// folded (`@tauri-apps/api` -> `@tauri-apps`), Go modules to their repo.
pub fn package_group(name: &str) -> String {
    const ALIASES: &[(&str, &str)] = &[
        ("microsoft.entityframeworkcore", "EF Core"),
        ("microsoft.aspnetcore", "ASP.NET Core"),
        ("microsoft.extensions", ".NET Extensions"),
        ("grpc.", "gRPC"),
        ("google.protobuf", "Protobuf"),
        ("opentelemetry", "OpenTelemetry"),
    ];
    let l = name.to_ascii_lowercase();
    for (prefix, alias) in ALIASES {
        if l.starts_with(prefix) {
            return alias.to_string();
        }
    }
    if let Some(scope) = name.strip_prefix('@') {
        return format!("@{}", scope.split('/').next().unwrap_or(scope));
    }
    if name.contains('/') {
        // A Go module path: host/org/repo.
        return name.split('/').take(3).collect::<Vec<_>>().join("/");
    }
    let segs: Vec<&str> = name.split('.').collect();
    if segs.len() > 1 {
        let keep = match segs[0] {
            "Azure" => 3,
            "Microsoft" | "System" => 2,
            _ => 1,
        };
        return segs[..keep.min(segs.len())].join(".");
    }
    name.to_string()
}

/// Resolve every unit's declared dependencies to units:
/// `(from, to, evidence)`, one per declaration, self-references dropped.
pub fn resolve_deps(units: &[Unit]) -> Vec<(usize, usize, Evidence)> {
    let mut by_name: HashMap<(Eco, String), Vec<usize>> = HashMap::new();
    for (i, u) in units.iter().enumerate() {
        for n in &u.names {
            by_name.entry(n.clone()).or_default().push(i);
        }
    }
    // Paths compare without case: Windows tools write `..\Libs\core\Core.csproj`
    // for `libs/Core/Core.csproj`.
    let mut by_manifest: HashMap<String, usize> = HashMap::new();
    let mut by_root: HashMap<String, usize> = HashMap::new();
    for (i, u) in units.iter().enumerate() {
        for m in &u.manifests {
            by_manifest.entry(m.to_lowercase()).or_insert(i);
        }
        if u.file_roots.is_empty() {
            by_root.entry(u.root.to_lowercase()).or_insert(i);
        }
    }
    let mut out = BTreeSet::new();
    for (i, u) in units.iter().enumerate() {
        for (dep, ev) in &u.deps {
            let targets: Vec<usize> = match dep {
                DepRef::Name(eco, n) => {
                    let key = match eco {
                        Eco::Py => (*eco, py_norm(n)),
                        Eco::Npm | Eco::Go => (*eco, n.clone()),
                        _ => (*eco, norm(n)),
                    };
                    by_name.get(&key).cloned().unwrap_or_default()
                }
                DepRef::Path(p) => {
                    // A manifest file names its unit; a directory the unit
                    // rooted there, else the project around it. A path no
                    // project holds (a missing or skipped one) links
                    // nothing, not the repository root or a loose folder.
                    let p = p.to_lowercase();
                    let dir = if p.ends_with('/') || p.is_empty() {
                        p.clone()
                    } else {
                        format!("{p}/")
                    };
                    by_manifest
                        .get(&p)
                        .copied()
                        .or_else(|| {
                            ancestors(&dir).find_map(|a| {
                                let &t = by_root.get(a)?;
                                let ok =
                                    a == dir || (!a.is_empty() && !units[t].manifests.is_empty());
                                Some(ok.then_some(t))
                            })?
                        })
                        .into_iter()
                        .collect()
                }
            };
            for t in targets {
                if t != i {
                    out.insert((i, t, ev.clone()));
                }
            }
        }
    }
    out.into_iter().collect()
}

/// External packages: what each unit declares, minus local packages.
pub fn external_packages(units: &[Unit]) -> Vec<BTreeSet<String>> {
    let local: HashSet<String> = units
        .iter()
        .flat_map(|u| u.names.iter().map(|(_, n)| norm(n)))
        .collect();
    units
        .iter()
        .map(|u| {
            u.packages
                .iter()
                .filter(|p| !local.contains(&norm(p)))
                .cloned()
                .collect()
        })
        .collect()
}

/// The names a unit goes by in full: its name, package names, and the
/// folders of its root and Dockerfiles (normalised).
fn full_names(u: &Unit) -> BTreeSet<String> {
    let base = |d: &str| norm(d.trim_end_matches('/').rsplit('/').next().unwrap_or(""));
    let mut out: BTreeSet<String> = [norm(&u.name), base(&u.root)].into();
    out.extend(
        u.names
            .iter()
            .map(|(_, n)| norm(n.rsplit('/').next().unwrap_or(n))),
    );
    out.extend(u.dockerfiles.iter().map(|d| base(dir_of(d))));
    out.remove("");
    out
}

/// `infra -> app` deployments: an image (or compose build directory) that
/// names an app. Evidence is the line that names it.
pub fn deployments(units: &[Unit]) -> Vec<(usize, usize, Evidence)> {
    let owners = Owners::new(units);
    let mut out = BTreeSet::new();
    for (i, u) in units.iter().enumerate() {
        for (image, ev) in &u.images {
            let targets: Vec<usize> = if let Some(dir) = image.strip_prefix("dir:") {
                owners.owner(dir).into_iter().collect()
            } else {
                let mut t: Vec<usize> = units
                    .iter()
                    .enumerate()
                    .filter(|(_, t)| {
                        t.kind != Kind::Infra && !t.kind.hidden() && t.aliases.contains(image)
                    })
                    .map(|(j, _)| j)
                    .collect();
                // Image `api` matches every `*.Api` project by its last
                // segment: when several match, keep the ones it names in
                // full (name or folder), and none when it names none.
                if t.len() > 1 {
                    t.retain(|&j| full_names(&units[j]).contains(image));
                }
                t
            };
            for t in targets {
                if t != i && units[t].kind != Kind::Infra {
                    out.insert((i, t, ev.clone()));
                }
            }
        }
    }
    out.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn helpers() {
        assert_eq!(norm("Foo.Api"), "fooapi");
        assert_eq!(tokens("Shop.Tests.Common"), ["shop", "tests", "common"]);
        assert_eq!(tokens("fooBarTests"), ["foo", "bar", "tests"]);
        assert_eq!(
            join_rel("a/b/", "..\\c\\c.csproj").as_deref(),
            Some("a/c/c.csproj")
        );
        assert_eq!(join_rel("a/", "../../x"), None);
        assert_eq!(
            image_name("reg.io/team/foo-api:1.2@sha256:ab").as_deref(),
            Some("fooapi")
        );
        assert_eq!(image_name("${IMAGE}"), None);
        assert_eq!(pep508_name("Foo_Bar[x]>=1"), "foo-bar");
        assert_eq!(
            package_group("Microsoft.EntityFrameworkCore.SqlServer"),
            "EF Core"
        );
        assert_eq!(
            package_group("Azure.Messaging.ServiceBus"),
            "Azure.Messaging.ServiceBus"
        );
        assert_eq!(package_group("@tauri-apps/api"), "@tauri-apps");
        assert_eq!(
            package_group("github.com/spf13/cobra/doc"),
            "github.com/spf13/cobra"
        );
        assert_eq!(
            xml_attr(r#"<Project Sdk="Microsoft.NET.Sdk.Web">"#, "Sdk"),
            Some("Microsoft.NET.Sdk.Web")
        );
        assert!(runs_workspace("turbo run dev"));
        assert!(runs_workspace("npx turbo dev --parallel"));
        assert!(runs_workspace("pnpm -r --parallel dev"));
        assert!(runs_workspace("npm run dev -w apps/web"));
        assert!(runs_workspace("yarn workspace web dev"));
        assert!(runs_workspace("pnpm --filter=web dev"));
        assert!(!runs_workspace("vite"));
        assert!(!runs_workspace("next dev"));
        assert!(!runs_workspace("node server.js && npm run watch"));
        let ancestors: Vec<&str> = ancestors("a/b/c.rs").collect();
        assert_eq!(ancestors, ["a/b/", "a/", ""]);
    }

    #[test]
    fn owners_pick_the_innermost_root() {
        let units = vec![
            Unit::new("outer", Kind::Lib, "a/", ""),
            Unit::new("inner", Kind::Lib, "a/b/", ""),
            Unit::new("root", Kind::Loose, "", ""),
        ];
        let o = Owners::new(&units);
        assert_eq!(o.owner("a/b/c/d.rs"), Some(1));
        assert_eq!(o.owner("a/x.rs"), Some(0));
        assert_eq!(o.owner("a/b/"), Some(1));
        assert_eq!(o.owner("z.rs"), Some(2));
    }
}
