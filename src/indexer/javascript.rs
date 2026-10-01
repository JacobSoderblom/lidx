use crate::db::resolver::{LanguageProfile, VisibilityRule};
use crate::indexer::channel;
use crate::indexer::config;
use crate::indexer::extract::{EdgeInput, ExtractedFile, ReceiverType, SymbolInput};
use crate::indexer::http;
use crate::indexer::proto;
use crate::indexer::tree_helpers::{
    collapse_call_target_whitespace, module_symbol_fallback, module_symbol_with_span, node_text,
    span,
};
use crate::util;
use anyhow::Result;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tree_sitter::{Node, Parser};

/// JavaScript/TypeScript's resolution profile: the shared default, plus a
/// recorded-visibility rule — `handle_method` records `visibility =
/// "private"` for an explicit `private` accessibility modifier or a
/// `#`-prefixed class field (see `is_private_member`), and
/// `mark_unexported_private` records a top-level symbol private unless the
/// file exports it (ESM `export`, `export { a }`, CommonJS
/// `module.exports`/`exports.x` — issue #151). Registered for "javascript",
/// "typescript" and "tsx" alike (`db::resolver::profile_for`) since they
/// share one resolution family.
pub(crate) const PROFILE: LanguageProfile = LanguageProfile {
    import_member_fallback: true,
    visibility: VisibilityRule::Recorded,
    ..LanguageProfile::DEFAULT
};

const JS_TS_EXTENSIONS: &[&str] = &["js", "jsx", "mjs", "cjs", "ts", "tsx", "mts", "cts", "d.ts"];
const HTTP_METHOD_NAMES: &[&str] = &[
    "get", "post", "put", "patch", "delete", "options", "head", "all",
];
const ROUTER_RECEIVERS: &[&str] = &["app", "router", "fastify", "server", "api", "koa"];
const GRPC_JS_RAW_METHODS: &[&str] = &[
    "makeUnaryRequest",
    "makeServerStreamRequest",
    "makeClientStreamRequest",
    "makeBidiStreamRequest",
];
const GRPC_JS_SKIP_METHODS: &[&str] = &[
    "close",
    "getChannel",
    "waitForReady",
    "makeUnaryRequest",
    "makeServerStreamRequest",
    "makeClientStreamRequest",
    "makeBidiStreamRequest",
];

#[derive(Clone, Debug)]
struct GrpcService {
    package: Option<String>,
    service: String,
}

#[derive(Clone)]
struct Context {
    /// Same-file string constants (see `string_consts`), used to resolve
    /// channel topics given as identifiers.
    string_consts: Rc<channel::StringConsts>,
    module: String,
    class_stack: Vec<String>,
    /// How many leading `class_stack` entries are TS namespaces rather than
    /// classes (`namespace A.B {}` pushes two).
    ns_depth: usize,
    fn_depth: usize,
    current_scope: String,
    route_prefix: Option<String>,
    router_aliases: Vec<String>,
    /// Same-file `const api = axios.create({ baseURL })` instances: name to
    /// the normalized static base path, when there is one.
    axios_instances: Rc<HashMap<String, Option<String>>>,
    grpc_clients: HashMap<String, GrpcService>,
    /// Types of locally-bound names (parameters + `const`/`let`/`var`
    /// declarations) within the *current* function body only — see
    /// `infer_local_types`. Reset fresh on every function/method entry;
    /// never merged across functions. See `python::infer_receiver_type` for
    /// the mechanism this mirrors.
    local_types: Rc<HashMap<String, LocalType>>,
    /// Type-annotated fields and constructor parameter properties of the
    /// *directly* enclosing class, read once when entering the class body —
    /// see `collect_class_level_attr_types`. Used only to resolve a
    /// single-hop `this.field.method()` receiver.
    class_attr_types: Rc<HashMap<String, LocalType>>,
    /// Qualname of the module-level `const`/`let`/`var` symbol whose
    /// initializer is currently being walked. The first function-like node
    /// (arrow, `function`/`function*` expression, object-literal method)
    /// met inside that initializer becomes a scope owned by this symbol —
    /// see `owned_function_scope`. Calls in the initializer *outside* any
    /// function (`const x = f()`, the `dynamic(...)` in `const X =
    /// dynamic(() => ...)`) still attribute to the module.
    fn_owner: Option<String>,
    /// This file's top-level `import` bindings — see `collect_import_bindings`.
    import_bindings: Rc<ImportBindings>,
}

/// Local name bound by a top-level `import` → (module specifier, imported
/// export name). The export name is `None` for a namespace import (`* as
/// ns`) and `"default"` for a default import (`import x from`, `import {
/// default as x }`); `import_placeholder` marks the latter so
/// `resolve_import_file_edges` can look up the target's actual default
/// export name, falling back to the local name when it can't.
type ImportBindings = HashMap<String, (String, Option<String>)>;

/// Separates specifier from imported member in the placeholder candidates
/// `handle_call` records; `resolve_import_file_edges` rewrites each into a
/// real qualname once the specifier can be resolved against the repo.
const IMPORT_PLACEHOLDER_SEP: char = '\0';

/// Prefixes a placeholder member for a default import; the local name
/// follows (`\u{1}api.get`).
const DEFAULT_IMPORT_MARK: char = '\u{1}';

/// Locally-inferred type of a name bound within a single function body (or
/// module top level). Deliberately coarse — see
/// `python::LocalType` for the shape this mirrors; everything that isn't a
/// confident, non-builtin type name collapses to `Other`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LocalType {
    /// Inferred (via type annotation or `new T()` construction) to be this
    /// non-builtin type name.
    Known(String),
    /// Builtin type, untyped parameter, destructured binding, loop/catch
    /// target, or anything else not explicitly recognized. A name landing
    /// here (rather than simply absent from the map) still gates
    /// resolution: it means "we looked, and it's not a usable type" as
    /// opposed to "we never looked".
    Other,
}

pub struct JavascriptExtractor {
    parser: Parser,
}

pub struct TypescriptExtractor {
    parser: Parser,
}

pub struct TsxExtractor {
    parser: Parser,
}

impl JavascriptExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_javascript::LANGUAGE;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }
}

impl crate::indexer::extract::LanguageExtractor for JavascriptExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String {
        module_name_from_rel_path(rel_path)
    }

    fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        extract_with_parser(&mut self.parser, source, module_name)
    }

    fn resolve_imports(
        &self,
        repo_root: &Path,
        file_rel_path: &str,
        module_name: &str,
        edges: &mut Vec<crate::indexer::extract::EdgeInput>,
    ) {
        resolve_import_file_edges(repo_root, file_rel_path, module_name, edges);
    }
}

impl TypescriptExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_typescript::LANGUAGE_TYPESCRIPT;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }
}

impl crate::indexer::extract::LanguageExtractor for TypescriptExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String {
        module_name_from_rel_path(rel_path)
    }

    fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        extract_with_parser(&mut self.parser, source, module_name)
    }

    fn resolve_imports(
        &self,
        repo_root: &Path,
        file_rel_path: &str,
        module_name: &str,
        edges: &mut Vec<crate::indexer::extract::EdgeInput>,
    ) {
        resolve_import_file_edges(repo_root, file_rel_path, module_name, edges);
    }
}

impl TsxExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_typescript::LANGUAGE_TSX;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }
}

impl crate::indexer::extract::LanguageExtractor for TsxExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String {
        module_name_from_rel_path(rel_path)
    }

    fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        extract_with_parser(&mut self.parser, source, module_name)
    }

    fn resolve_imports(
        &self,
        repo_root: &Path,
        file_rel_path: &str,
        module_name: &str,
        edges: &mut Vec<crate::indexer::extract::EdgeInput>,
    ) {
        resolve_import_file_edges(repo_root, file_rel_path, module_name, edges);
    }
}

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "index".to_string();
    }
    let file = parts.pop().unwrap_or_default();
    let mut stem = Path::new(&file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(&file)
        .to_string();
    if stem.ends_with(".d") {
        stem.truncate(stem.len() - 2);
    }
    if stem != "index" {
        parts.push(stem);
    }
    if parts.is_empty() {
        "index".to_string()
    } else {
        parts.join("/")
    }
}

pub fn resolve_import_file_edges(
    repo_root: &Path,
    file_rel_path: &str,
    _file_module: &str,
    edges: &mut Vec<EdgeInput>,
) {
    // Rewrite `handle_call`'s placeholder import candidates into the
    // imported symbol's qualname in the resolved file. A specifier that
    // doesn't resolve to a repo file (`react`, `next/navigation`, a missing
    // relative file) keeps a `{specifier}:{member}` candidate that can never
    // match a symbol: its only job is to keep the list non-empty so
    // `Db::insert_edges` refuses fuzzy resolution for a call known to go
    // through an import.
    //
    // Re-exports (`export { x } from`, `export * from`) and default exports
    // are chased on disk (`chase_export`), so the candidate names the
    // original declaration rather than the barrel.
    let mut resolved_specs: HashMap<String, Option<String>> = HashMap::new();
    for edge in edges.iter_mut() {
        for candidate in edge.import_candidates.iter_mut() {
            let Some((spec, member)) = candidate.split_once(IMPORT_PLACEHOLDER_SEP) else {
                continue;
            };
            let dst = resolved_specs
                .entry(spec.to_string())
                .or_insert_with(|| resolve_import_path(repo_root, file_rel_path, spec));
            *candidate = match dst {
                Some(dst) => {
                    let (path, member) = EXPORT_CACHE
                        .with(|c| chase_member(repo_root, dst, member, &mut c.borrow_mut()));
                    format!("{}.{member}", module_name_from_rel_path(&path))
                }
                None => format!("{spec}:{}", member.trim_start_matches(DEFAULT_IMPORT_MARK)),
            };
        }
    }
    let mut resolved = Vec::new();
    for edge in edges.iter() {
        if edge.kind != "IMPORTS" {
            continue;
        }
        let Some(raw_target) = edge.target_qualname.as_deref() else {
            continue;
        };
        let Some((target, is_relative)) = classify_import_target(raw_target) else {
            continue;
        };
        // Issue #77: a relative specifier always yields an IMPORTS_FILE
        // edge, whether or not its target currently resolves to a real
        // file — like CALLS emits an unresolved placeholder. A bare
        // specifier (third-party, or an unmapped alias) still needs
        // `resolve_tsconfig_alias`'s disk-backed lookup, since there's no
        // repo-relative path to guess without it.
        let Some((dst_rel, resolved_on_disk)) =
            resolve_import_target(repo_root, file_rel_path, target, is_relative)
        else {
            continue;
        };
        let dst_module = module_name_from_rel_path(&dst_rel);
        resolved.push(EdgeInput {
            kind: "IMPORTS_FILE".to_string(),
            source_qualname: edge.source_qualname.clone(),
            target_qualname: Some(dst_module),
            detail: Some(
                json!({
                    "src_path": file_rel_path,
                    "dst_path": dst_rel,
                    "confidence": if resolved_on_disk { 1.0 } else { 0.0 },
                })
                .to_string(),
            ),
            evidence_snippet: edge.evidence_snippet.clone(),
            evidence_start_line: edge.evidence_start_line,
            evidence_end_line: edge.evidence_end_line,
            ..Default::default()
        });
    }
    edges.extend(resolved);
}

/// Name of the default export in `chase_export` lookups, and of the symbol an
/// anonymous default export (`export default () => ..`, `export default
/// class {}`, `export default { .. }`) is indexed under.
const DEFAULT_EXPORT: &str = "default";

type ExportCache = HashMap<String, Option<Rc<FileExports>>>;

thread_local! {
    /// Parsed export tables shared by every `resolve_import_file_edges` call
    /// in one sync/reindex batch, so a barrel is parsed once per batch. The
    /// indexer clears it at each batch boundary (`clear_export_cache`).
    static EXPORT_CACHE: std::cell::RefCell<ExportCache> = std::cell::RefCell::new(HashMap::new());
}

/// Drops the per-batch export cache; called by the indexer at the start and
/// end of every sync/reindex so no stale barrel survives a file edit.
pub fn clear_export_cache() {
    EXPORT_CACHE.with(|c| c.borrow_mut().clear());
}

/// Whether `rel_path` is a JS/TS source file (by extension).
pub fn is_js_ts_path(rel_path: &str) -> bool {
    JS_TS_EXTENSIONS
        .iter()
        .any(|ext| rel_path.ends_with(&format!(".{ext}")))
}

/// `export { orig as exported } from spec`.
struct ReExport {
    exported: String,
    spec: String,
    orig: String,
}

/// What a file exports, for chasing an import through barrels.
#[derive(Default)]
struct FileExports {
    /// Local names exported under their own name.
    names: HashSet<String>,
    /// `export { a as b }`: exported `b` -> local `a`.
    aliases: HashMap<String, String>,
    /// Local name of the default export, when it is a named declaration.
    default_local: Option<String>,
    reexports: Vec<ReExport>,
    /// `export * from spec`.
    stars: Vec<String>,
    /// `export * as ns from spec`: exported `ns` -> spec.
    namespaces: HashMap<String, String>,
    /// This file's own imports, so `import { Foo } from './foo'; export {
    /// Foo }` chases on to `./foo`.
    imports: ImportBindings,
}

impl FileExports {
    /// Whether this file passes other modules' exports on (a barrel), so a
    /// change in one of its imports can change what its importers resolve.
    fn re_exports(&self) -> bool {
        if !self.reexports.is_empty() || !self.stars.is_empty() || !self.namespaces.is_empty() {
            return true;
        }
        self.default_local
            .iter()
            .chain(self.aliases.values())
            .chain(self.names.iter())
            .any(|local| self.imports.contains_key(local))
    }

    /// Stable text of everything `chase_export` reads, for `export_surface_hash`.
    fn surface_text(&self) -> String {
        fn sorted<T: Ord + std::fmt::Debug>(items: impl Iterator<Item = T>) -> String {
            let mut v: Vec<T> = items.collect();
            v.sort();
            format!("{v:?}")
        }
        format!(
            "{}|{}|{:?}|{}|{}|{}|{}",
            sorted(self.names.iter()),
            sorted(self.aliases.iter()),
            self.default_local,
            sorted(
                self.reexports
                    .iter()
                    .map(|r| (&r.exported, &r.spec, &r.orig))
            ),
            sorted(self.stars.iter()),
            sorted(self.namespaces.iter()),
            sorted(self.imports.iter().map(|(k, (s, i))| (k, s, i))),
        )
    }
}

fn scan_exports(repo_root: &Path, rel: &str) -> Option<FileExports> {
    let source = util::read_to_string(&repo_root.join(rel)).ok()?;
    let mut parser = Parser::new();
    let language = match Path::new(rel).extension().and_then(|e| e.to_str()) {
        Some("ts" | "mts" | "cts") => tree_sitter_typescript::LANGUAGE_TYPESCRIPT,
        Some("tsx") => tree_sitter_typescript::LANGUAGE_TSX,
        _ => tree_sitter_javascript::LANGUAGE,
    };
    parser.set_language(&language.into()).ok()?;
    let tree = parser.parse(&source, None)?;
    Some(exports_from_root(tree.root_node(), &source))
}

fn exports_from_root(root: Node<'_>, source: &str) -> FileExports {
    let source = source.to_string();
    let mut ex = Exports::default();
    let mut out = FileExports {
        imports: collect_import_bindings(root, &source),
        ..Default::default()
    };
    let mut cursor = root.walk();
    for stmt in root.named_children(&mut cursor) {
        collect_exported_names(stmt, &source, &mut ex);
        if !matches!(stmt.kind(), "export_statement" | "export_declaration") {
            continue;
        }
        let spec = stmt
            .child_by_field_name("source")
            .and_then(|n| unquote_string_literal(&node_text(n, &source)));
        let mut c = stmt.walk();
        let is_default = stmt.children(&mut c).any(|ch| ch.kind() == "default");
        if is_default {
            let named = stmt
                .child_by_field_name("declaration")
                .and_then(|d| d.child_by_field_name("name"))
                .or_else(|| {
                    stmt.child_by_field_name("value")
                        .filter(|v| v.kind() == "identifier")
                });
            match named {
                Some(n) => out.default_local = Some(node_text(n, &source)),
                // `export default <expr>` / `export default function () {}`:
                // the symbol `handle_anonymous_default` emits.
                None if stmt.child_by_field_name("value").is_some() => {
                    out.default_local = Some(DEFAULT_EXPORT.to_string());
                }
                None => {}
            }
        }
        let mut c = stmt.walk();
        for child in stmt.children(&mut c) {
            match child.kind() {
                "*" => out.stars.extend(spec.clone()),
                "namespace_export" => {
                    if let (Some(spec), Some(name)) = (&spec, child.named_child(0)) {
                        out.namespaces
                            .insert(node_text(name, &source), spec.clone());
                    }
                }
                "export_clause" => {
                    let mut sc = child.walk();
                    for item in child.named_children(&mut sc) {
                        let Some(name) = item.child_by_field_name("name") else {
                            continue;
                        };
                        let orig = node_text(name, &source);
                        let exported = item
                            .child_by_field_name("alias")
                            .map(|a| node_text(a, &source))
                            .unwrap_or_else(|| orig.clone());
                        match &spec {
                            Some(spec) => out.reexports.push(ReExport {
                                exported,
                                spec: spec.clone(),
                                orig,
                            }),
                            None if exported == DEFAULT_EXPORT => out.default_local = Some(orig),
                            None => {
                                out.aliases.insert(exported, orig);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    out.names = ex.names;
    out
}

/// Hash of what `chase_export` can see of `rel` (its export surface), or
/// `None` when the file can't be read. A body-only edit leaves it unchanged,
/// so importers chased through the file needn't be re-extracted.
pub fn export_surface_hash(repo_root: &Path, rel: &str) -> Option<i64> {
    let exports = scan_exports(repo_root, rel)?;
    Some(crate::indexer::scan::hash_i64(
        exports.surface_text().as_bytes(),
    ))
}

/// Whether `rel` re-exports other modules' exports (see
/// `FileExports::re_exports`); `false` when unreadable.
pub fn is_re_exporting(repo_root: &Path, rel: &str) -> bool {
    scan_exports(repo_root, rel).is_some_and(|e| e.re_exports())
}

/// What an export resolves to.
#[derive(PartialEq, Eq)]
enum ExportTarget {
    /// A declaration: file path and local name.
    Symbol(String, String),
    /// A whole module (`export * as ns from`, or an exported namespace import).
    Namespace(String),
}

/// (file path, export name) pairs already explored by one chase; each is
/// explored once, which also terminates cycles and diamond re-walks.
type Visited = HashSet<(String, String)>;

/// Follows export `name` (`"default"` for the default export) of `path`
/// through `export ... from` chains and import-then-export barrels to what
/// declares it. `None` when nothing is found.
fn chase_export(
    repo_root: &Path,
    path: &str,
    name: &str,
    cache: &mut ExportCache,
    visited: &mut Visited,
) -> Option<ExportTarget> {
    if !visited.insert((path.to_string(), name.to_string())) {
        return None;
    }
    let exports = cache
        .entry(path.to_string())
        .or_insert_with(|| scan_exports(repo_root, path).map(Rc::new))
        .clone()?;
    let mut follow = |spec: &str, orig: &str| {
        let dst = resolve_import_path(repo_root, path, spec)?;
        chase_export(repo_root, &dst, orig, cache, visited)
    };
    // A local name this file exports: an import binding chases on to the
    // imported module, anything else is declared here.
    let local_target = |local: &str, follow: &mut dyn FnMut(&str, &str) -> Option<ExportTarget>| {
        if let Some((spec, imported)) = exports.imports.get(local) {
            let hit = match imported {
                Some(orig) => follow(spec, orig),
                None => resolve_import_path(repo_root, path, spec).map(ExportTarget::Namespace),
            };
            if hit.is_some() {
                return hit;
            }
        }
        Some(ExportTarget::Symbol(path.to_string(), local.to_string()))
    };
    if name == DEFAULT_EXPORT
        && let Some(local) = &exports.default_local
    {
        return local_target(local, &mut follow);
    }
    for r in &exports.reexports {
        if r.exported == name
            && let Some(hit) = follow(&r.spec, &r.orig)
        {
            return Some(hit);
        }
    }
    if let Some(spec) = exports.namespaces.get(name)
        && let Some(dst) = resolve_import_path(repo_root, path, spec)
    {
        return Some(ExportTarget::Namespace(dst));
    }
    if let Some(local) = exports.aliases.get(name) {
        return local_target(local, &mut follow);
    }
    if name == DEFAULT_EXPORT {
        return None;
    }
    if exports.names.contains(name) {
        return local_target(name, &mut follow);
    }
    // `export *` sources: two that both provide `name` collide, and a
    // colliding name is exported by neither.
    let mut found: Option<ExportTarget> = None;
    for spec in &exports.stars {
        let Some(hit) = follow(spec, name) else {
            continue;
        };
        match &found {
            None => found = Some(hit),
            Some(prev) if *prev == hit => {}
            Some(_) => return None,
        }
    }
    found
}

/// Rewrites an import placeholder's `member` (`name[.rest]`, or a
/// `DEFAULT_IMPORT_MARK`-prefixed `local[.rest]` for a default import)
/// against `dst`, returning the declaring file and the plain member.
/// Anything not found keeps `dst` and the imported/local name.
fn chase_member(
    repo_root: &Path,
    dst: &str,
    member: &str,
    cache: &mut ExportCache,
) -> (String, String) {
    let (is_default, member) = match member.strip_prefix(DEFAULT_IMPORT_MARK) {
        Some(m) => (true, m),
        None => (false, member),
    };
    let (head, tail) = match member.split_once('.') {
        Some((h, t)) => (h, format!(".{t}")),
        None => (member, String::new()),
    };
    let lookup = if is_default { DEFAULT_EXPORT } else { head };
    let fallback = || (dst.to_string(), member.to_string());
    match chase_export(repo_root, dst, lookup, cache, &mut Visited::new()) {
        Some(ExportTarget::Symbol(path, name)) => (path, format!("{name}{tail}")),
        // `ns.fn()` through an exported namespace: chase `fn` in that module.
        Some(ExportTarget::Namespace(module)) => {
            let Some((next, rest)) = tail.strip_prefix('.').map(|t| match t.split_once('.') {
                Some((n, r)) => (n.to_string(), format!(".{r}")),
                None => (t.to_string(), String::new()),
            }) else {
                return fallback();
            };
            match chase_export(repo_root, &module, &next, cache, &mut Visited::new()) {
                Some(ExportTarget::Symbol(path, name)) => (path, format!("{name}{rest}")),
                _ => fallback(),
            }
        }
        None => fallback(),
    }
}

/// Splits off any `?query`/`#hash` suffix and classifies whether `target`
/// is a relative specifier (`./`, `../`, or a repo-absolute `/`) — `None`
/// for an empty specifier. Shared by `resolve_import_path`'s disk-backed
/// resolution and `resolve_import_file_edges`'s disk-independent fallback
/// for a relative specifier that doesn't currently resolve to a file
/// (issue #77: an `IMPORTS_FILE` edge is still emitted then, just
/// unresolved, rather than omitted).
fn classify_import_target(target: &str) -> Option<(&str, bool)> {
    let target = target.split(['?', '#']).next().unwrap_or(target).trim();
    if target.is_empty() {
        return None;
    }
    let is_relative =
        target.starts_with("./") || target.starts_with("../") || target.starts_with('/');
    Some((target, is_relative))
}

/// The literal repo-relative path a relative specifier (already classified
/// by `classify_import_target`) points at, lexically collapsing `..` so
/// `components/../lib/utils` yields the same path as `lib/utils` — before
/// any extension/index-file probing. `None` only for one that walks `..`
/// past the repo root.
fn relative_import_target(file_rel_path: &str, target: &str) -> Option<PathBuf> {
    let base_dir = Path::new(file_rel_path)
        .parent()
        .unwrap_or_else(|| Path::new(""));
    let joined = if target.starts_with('/') {
        PathBuf::from(target.trim_start_matches('/'))
    } else {
        base_dir.join(target)
    };
    let mut rel = PathBuf::new();
    for comp in joined.components() {
        match comp {
            std::path::Component::ParentDir => {
                if !rel.pop() {
                    return None;
                }
            }
            std::path::Component::Normal(part) => rel.push(part),
            _ => {}
        }
    }
    Some(rel)
}

fn resolve_import_path(repo_root: &Path, file_rel_path: &str, target: &str) -> Option<String> {
    let (target, is_relative) = classify_import_target(target)?;
    let (path, on_disk) = resolve_import_target(repo_root, file_rel_path, target, is_relative)?;
    on_disk.then_some(path)
}

/// Resolves an already-classified specifier (`classify_import_target`'s
/// output) to a repo-relative path: `(path, true)` when it names a real
/// file on disk, `(path, false)` only for a relative specifier that
/// doesn't (there's still a repo-relative path worth guessing), `None`
/// when nothing usable exists at all — an unmapped alias/third-party
/// specifier, or a relative specifier that walks past the repo root.
///
/// Not a relative specifier: it's either a genuine third-party import
/// (e.g. `next/navigation`) or an alias remapped through the owning
/// tsconfig.json's `compilerOptions.paths` (e.g. `@/lib/foo`). Only the
/// latter ever resolves, and only when a concrete path-mapping entry backs
/// it *and* the mapped location is a real file — no fuzzy fallback.
///
/// Shared by `resolve_import_path` (collapses to `Option<String>`, for
/// rewriting a call's import-candidate placeholder) and
/// `resolve_import_file_edges` (keeps the on-disk flag, since a relative
/// specifier still gets an unresolved `IMPORTS_FILE` edge rather than none
/// at all — issue #77).
fn resolve_import_target(
    repo_root: &Path,
    file_rel_path: &str,
    target: &str,
    is_relative: bool,
) -> Option<(String, bool)> {
    if !is_relative {
        return resolve_tsconfig_alias(repo_root, file_rel_path, target);
    }
    let rel = relative_import_target(file_rel_path, target)?;
    match probe_module_candidates(repo_root, &rel) {
        Some(found) => Some((found, true)),
        None => Some((util::normalize_path(&rel), false)),
    }
}

/// Checks whether `rel` (extension-less or not, relative to `repo_root`)
/// names a real source file, trying it as given, each JS/TS extension, and
/// each extension under an `index` file in that directory — the same three
/// tiers Node/TypeScript module resolution tries for a relative specifier.
/// Shared by plain relative imports and by tsconfig alias resolution so
/// both go through identical, filesystem-verified matching.
fn probe_module_candidates(repo_root: &Path, rel: &Path) -> Option<String> {
    if rel.extension().is_some() {
        if repo_root.join(rel).is_file() {
            return Some(util::normalize_path(rel));
        }
        // NodeNext/ESM TypeScript imports the emitted name: `./x.js` is
        // `x.ts` on disk (likewise .jsx→.tsx, .mjs→.mts, .cjs→.cts).
        let ts_ext = match rel.extension().and_then(|e| e.to_str()) {
            Some("js") => &["ts", "tsx"][..],
            Some("jsx") => &["tsx"][..],
            Some("mjs") => &["mts"][..],
            Some("cjs") => &["cts"][..],
            _ => &[][..],
        };
        return ts_ext
            .iter()
            .map(|ext| rel.with_extension(ext))
            .find(|candidate| repo_root.join(candidate).is_file())
            .map(|candidate| util::normalize_path(&candidate));
    }
    for ext in JS_TS_EXTENSIONS {
        let candidate = rel.with_extension(ext);
        if repo_root.join(&candidate).is_file() {
            return Some(util::normalize_path(&candidate));
        }
    }
    for ext in JS_TS_EXTENSIONS {
        let candidate = rel.join("index").with_extension(ext);
        if repo_root.join(&candidate).is_file() {
            return Some(util::normalize_path(&candidate));
        }
    }
    None
}

/// Resolves a non-relative import specifier (`@/lib/foo`) through the
/// nearest ancestor `tsconfig.json`'s `compilerOptions.paths`, scoped to
/// that config's own directory (and its `baseUrl`) so two sibling projects
/// with their own tsconfigs — e.g. `node/datacatalog-ui` and
/// `node/dpb-app`, each mapping `@/*` to a different root — never bleed
/// into each other.
///
/// Returns `None` unless a `paths` entry syntactically matches the
/// specifier. Then `(path, true)` when the mapped location, run back through
/// the same extension/index probing relative imports use, is a real file,
/// else `(guess, false)`: the first mapped location, so the caller can still
/// record an unresolved `IMPORTS_FILE` edge that a later-added file (or a
/// tsconfig edit) finds again (`Indexer::js_importers_of`).
fn resolve_tsconfig_alias(
    repo_root: &Path,
    file_rel_path: &str,
    target: &str,
) -> Option<(String, bool)> {
    let config_dir = find_owning_tsconfig_dir(repo_root, file_rel_path)?;
    let aliases = load_tsconfig_aliases(repo_root, &config_dir)?;
    let mut guess: Option<String> = None;
    for (pattern, targets) in &aliases.entries {
        let Some(capture) = match_alias_pattern(pattern, target) else {
            continue;
        };
        let pattern_has_star = pattern.contains('*');
        for target_template in targets {
            let Some(mapped_tail) =
                substitute_alias_target(target_template, &capture, pattern_has_star)
            else {
                continue;
            };
            let mut rel = aliases.base_dir.clone();
            rel.push(mapped_tail);
            // Collapse `..` (a base config's `../../lib/*`) so the resolved
            // path names the same module as a direct import of that file.
            let Some(rel) = relative_import_target("", &util::normalize_path(&rel)) else {
                continue;
            };
            if let Some(resolved) = probe_module_candidates(repo_root, &rel) {
                return Some((resolved, true));
            }
            guess.get_or_insert_with(|| util::normalize_path(&rel));
        }
    }
    guess.map(|g| (g, false))
}

/// Whether `rel_path` is a `tsconfig.json`/`jsconfig.json`, whose edits can
/// change how every JS/TS import under its directory resolves.
pub fn is_js_config_path(rel_path: &str) -> bool {
    matches!(
        Path::new(rel_path).file_name().and_then(|n| n.to_str()),
        Some("tsconfig.json" | "jsconfig.json")
    )
}

/// Walks from `file_rel_path`'s directory up toward `repo_root`, returning
/// the directory (relative to `repo_root`) of the nearest ancestor
/// `tsconfig.json`, if any. This is what makes alias resolution per-project
/// rather than global: a file under `node/dpb-app/` finds
/// `node/dpb-app/tsconfig.json` before it ever sees
/// `node/datacatalog-ui/tsconfig.json`, even though both define `@/*`.
pub fn find_owning_tsconfig_dir(repo_root: &Path, file_rel_path: &str) -> Option<PathBuf> {
    let start_dir = Path::new(file_rel_path)
        .parent()
        .unwrap_or_else(|| Path::new(""));
    for dir in start_dir.ancestors() {
        if repo_root.join(dir).join("tsconfig.json").is_file() {
            return Some(dir.to_path_buf());
        }
    }
    None
}

/// A tsconfig's `compilerOptions.paths`, parsed once per lookup: the
/// directory `paths` targets are resolved against (`baseUrl`, itself
/// resolved against the config's own directory — defaulting to that
/// directory when `baseUrl` is absent, per tsconfig semantics), and the
/// pattern/target entries in TypeScript's own longest-prefix-first order.
struct TsconfigAliases {
    base_dir: PathBuf,
    entries: Vec<(String, Vec<String>)>,
}

/// `compilerOptions.paths` as (pattern, targets) pairs.
type PathsEntries = Vec<(String, Vec<String>)>;

/// The `baseUrl`/`paths` a tsconfig ends up with after following its
/// `extends` chain: the nearest declaring config wins for each (a child's
/// `paths` replaces its parent's wholesale, as in TypeScript), and a
/// `baseUrl` is resolved against the config that declared it.
#[derive(Default)]
struct EffectiveOptions {
    base_url: Option<PathBuf>,
    /// `paths` entries plus the directory of the config that declared them
    /// (their base when no `baseUrl` is set anywhere).
    paths: Option<(PathBuf, PathsEntries)>,
}

fn read_tsconfig(repo_root: &Path, config_rel: &Path) -> Option<serde_json::Value> {
    let raw = util::read_to_string(&repo_root.join(config_rel)).ok()?;
    serde_json::from_str(&strip_jsonc(&raw)).ok()
}

/// `extends` entries of a parsed tsconfig: a string, or (TS 5) an array.
fn extends_specs(value: &serde_json::Value) -> Vec<String> {
    match value.get("extends") {
        Some(serde_json::Value::String(s)) => vec![s.clone()],
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        _ => Vec::new(),
    }
}

/// Repo-relative path of the config an `extends` spec names. A relative
/// spec always yields a path (even for a missing file, so a later-added
/// base still shows up in `config_chain`); a package-style spec
/// (`@tsconfig/node18/tsconfig.json`) is looked up under each ancestor's
/// `node_modules` and yields `None` unless found.
fn resolve_extends(repo_root: &Path, config_rel: &Path, spec: &str) -> Option<PathBuf> {
    let config_dir = config_rel.parent().unwrap_or_else(|| Path::new(""));
    let probe = |base: PathBuf| -> Option<PathBuf> {
        let plain = repo_root.join(&base);
        if plain.is_file() {
            return Some(base);
        }
        let with_json = PathBuf::from(format!("{}.json", util::normalize_path(&base)));
        if repo_root.join(&with_json).is_file() {
            return Some(with_json);
        }
        let index = base.join("tsconfig.json");
        repo_root.join(&index).is_file().then_some(index)
    };
    if spec.starts_with("./") || spec.starts_with("../") {
        let rel = relative_import_target(&util::normalize_path(config_rel), spec)?;
        let fallback = PathBuf::from(format!("{}.json", util::normalize_path(&rel)));
        return Some(probe(rel.clone()).unwrap_or(if spec.ends_with(".json") {
            rel
        } else {
            fallback
        }));
    }
    if spec.starts_with('/') || spec.is_empty() {
        return None;
    }
    config_dir
        .ancestors()
        .find_map(|dir| probe(dir.join("node_modules").join(spec)))
}

/// One link of a config's dependency chain (`config_chain`).
#[derive(Debug, PartialEq, Eq)]
pub enum ConfigRef {
    /// A config file (repo-relative path), present or not.
    File(String),
    /// A package-style `extends` spec that resolves to no file. Kept so its
    /// appearing or disappearing changes the fingerprint.
    MissingPackage(String),
}

/// Every config file `config_rel` depends on: itself plus its `extends`
/// chain, transitively and cycle-safe.
///
/// Sync only sees paths it is asked to sync and `node_modules` is usually
/// not watched, so an appearing/disappearing package base is caught by
/// `Indexer::reindex`, whose config fingerprint hashes this chain.
pub fn config_chain(repo_root: &Path, config_rel: &Path) -> Vec<ConfigRef> {
    fn walk(repo_root: &Path, config_rel: &Path, seen: &mut Vec<ConfigRef>) {
        let key = ConfigRef::File(util::normalize_path(config_rel));
        if seen.contains(&key) {
            return;
        }
        seen.push(key);
        let Some(value) = read_tsconfig(repo_root, config_rel) else {
            return;
        };
        for spec in extends_specs(&value) {
            match resolve_extends(repo_root, config_rel, &spec) {
                Some(parent) => walk(repo_root, &parent, seen),
                None => seen.push(ConfigRef::MissingPackage(spec)),
            }
        }
    }
    let mut seen = Vec::new();
    walk(repo_root, config_rel, &mut seen);
    seen
}

fn effective_options(
    repo_root: &Path,
    config_rel: &Path,
    visiting: &mut HashSet<PathBuf>,
) -> EffectiveOptions {
    let mut out = EffectiveOptions::default();
    if !visiting.insert(config_rel.to_path_buf()) {
        return out;
    }
    if let Some(value) = read_tsconfig(repo_root, config_rel) {
        let dir = config_rel.parent().unwrap_or_else(|| Path::new(""));
        for spec in extends_specs(&value) {
            if let Some(parent) = resolve_extends(repo_root, config_rel, &spec) {
                let inherited = effective_options(repo_root, &parent, visiting);
                out.base_url = inherited.base_url.or(out.base_url);
                out.paths = inherited.paths.or(out.paths);
            }
        }
        if let Some(compiler_options) = value.get("compilerOptions") {
            if let Some(base_url) = compiler_options.get("baseUrl").and_then(|v| v.as_str()) {
                out.base_url = Some(dir.join(base_url));
            }
            if let Some(paths) = compiler_options.get("paths").and_then(|v| v.as_object()) {
                let mut entries: Vec<(String, Vec<String>)> = Vec::new();
                for (pattern, targets_value) in paths {
                    let Some(targets_array) = targets_value.as_array() else {
                        continue;
                    };
                    let targets: Vec<String> = targets_array
                        .iter()
                        .filter_map(|t| t.as_str().map(|s| s.to_string()))
                        .collect();
                    if !targets.is_empty() {
                        entries.push((pattern.clone(), targets));
                    }
                }
                out.paths = Some((dir.to_path_buf(), entries));
            }
        }
    }
    visiting.remove(config_rel);
    out
}

fn load_tsconfig_aliases(repo_root: &Path, config_dir: &Path) -> Option<TsconfigAliases> {
    let effective = effective_options(
        repo_root,
        &config_dir.join("tsconfig.json"),
        &mut HashSet::new(),
    );
    let (paths_dir, mut entries) = effective.paths?;
    if entries.is_empty() {
        return None;
    }
    let base_dir = effective.base_url.unwrap_or(paths_dir);
    // TypeScript tries the pattern with the longest non-wildcard prefix
    // first when more than one pattern could match the same specifier.
    entries.sort_by(|(a, _), (b, _)| {
        let a_len = a.split('*').next().unwrap_or(a).len();
        let b_len = b.split('*').next().unwrap_or(b).len();
        b_len.cmp(&a_len)
    });
    Some(TsconfigAliases { base_dir, entries })
}

/// Matches a specifier against one `paths` pattern key (`"@/*"`, or an
/// exact key with no wildcard at all). A pattern with more than one `*` is
/// not a shape tsconfig itself allows — treated as malformed and refused
/// rather than guessed at. Returns the text the `*` captured (empty string
/// for an exact, wildcard-free match).
fn match_alias_pattern(pattern: &str, specifier: &str) -> Option<String> {
    match pattern.find('*') {
        Some(idx) => {
            let prefix = &pattern[..idx];
            let suffix = &pattern[idx + 1..];
            if suffix.contains('*') {
                return None;
            }
            if specifier.starts_with(prefix)
                && specifier.ends_with(suffix)
                && specifier.len() >= prefix.len() + suffix.len()
            {
                Some(specifier[prefix.len()..specifier.len() - suffix.len()].to_string())
            } else {
                None
            }
        }
        None => {
            if specifier == pattern {
                Some(String::new())
            } else {
                None
            }
        }
    }
}

/// Substitutes a pattern's captured wildcard text into one of its `paths`
/// targets (`"./*"`, `"./src/*"`, or an exact target). Refuses rather than
/// guesses when the target's wildcard shape doesn't match the pattern's
/// (tsconfig requires them to agree): a wildcard pattern needs a target
/// with exactly one `*` to substitute into, and an exact pattern needs an
/// exact (wildcard-free) target, since a literal target can't disambiguate
/// which file a wildcard capture meant.
fn substitute_alias_target(target: &str, capture: &str, pattern_has_star: bool) -> Option<String> {
    match target.find('*') {
        Some(idx) => {
            if !pattern_has_star || target[idx + 1..].contains('*') {
                return None;
            }
            let mut out = String::with_capacity(target.len() + capture.len());
            out.push_str(&target[..idx]);
            out.push_str(capture);
            out.push_str(&target[idx + 1..]);
            Some(out)
        }
        None => {
            if pattern_has_star {
                None
            } else {
                Some(target.to_string())
            }
        }
    }
}

/// Strips `//` and `/* */` comments from JSONC text (tsconfig.json's actual
/// format) so it parses as plain JSON, without disturbing comment-like text
/// inside string literals. Trailing commas before a closing `}`/`]` — the
/// other JSONC-ism tsconfig files sometimes carry — are dropped in the same
/// pass.
fn strip_jsonc(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' => {
                out.push(ch);
                while let Some(sch) = chars.next() {
                    out.push(sch);
                    if sch == '\\' {
                        if let Some(escaped) = chars.next() {
                            out.push(escaped);
                        }
                    } else if sch == '"' {
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'/') => {
                chars.next();
                for sch in chars.by_ref() {
                    if sch == '\n' {
                        out.push('\n');
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = '\0';
                for sch in chars.by_ref() {
                    if prev == '*' && sch == '/' {
                        break;
                    }
                    prev = sch;
                }
            }
            _ => out.push(ch),
        }
    }
    strip_trailing_commas(&out)
}

/// Removes a comma that (ignoring whitespace) is immediately followed by a
/// closing `}` or `]`, without touching commas inside string literals.
fn strip_trailing_commas(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '"' {
            out.push(ch);
            while let Some(sch) = chars.next() {
                out.push(sch);
                if sch == '\\' {
                    if let Some(escaped) = chars.next() {
                        out.push(escaped);
                    }
                } else if sch == '"' {
                    break;
                }
            }
            continue;
        }
        if ch == ',' {
            let next_significant = chars.clone().find(|c| !c.is_whitespace());
            if matches!(next_significant, Some('}') | Some(']')) {
                continue;
            }
        }
        out.push(ch);
    }
    out
}

fn extract_with_parser(
    parser: &mut Parser,
    source: &str,
    module_name: &str,
) -> Result<ExtractedFile> {
    let mut output = ExtractedFile::default();
    let tree = match parser.parse(source, None) {
        Some(tree) => tree,
        None => {
            output
                .symbols
                .push(module_symbol_fallback(module_name, source, "/", None));
            return Ok(output);
        }
    };
    let root = tree.root_node();

    let module_span = span(root);
    output
        .symbols
        .push(module_symbol_with_span(module_name, module_span, "/", None));
    if let Some(edge) = next_page_route_edge(module_name) {
        output.edges.push(edge);
    }
    output
        .edges
        .extend(next_api_route_edges(module_name, root, source));
    let grpc_clients = collect_grpc_clients(root, source);
    let ctx = Context {
        string_consts: Rc::new(crate::indexer::string_consts::collect_string_consts(
            crate::indexer::string_consts::ConstLang::JavaScript,
            root,
            source,
        )),
        module: module_name.to_string(),
        class_stack: Vec::new(),
        ns_depth: 0,
        fn_depth: 0,
        current_scope: module_name.to_string(),
        route_prefix: None,
        router_aliases: Vec::new(),
        axios_instances: Rc::new(collect_axios_instances(root, source)),
        grpc_clients,
        local_types: Rc::new(infer_module_level_types(root, source)),
        class_attr_types: Rc::new(HashMap::new()),
        fn_owner: None,
        import_bindings: Rc::new(collect_import_bindings(root, source)),
    };
    walk_node(root, &ctx, source, &mut output);
    dedup_namespace_symbols(&mut output);
    mark_unexported_private(root, source, module_name, &mut output);
    output.export_surface = Some(crate::indexer::scan::hash_i64(
        exports_from_root(root, source).surface_text().as_bytes(),
    ));
    Ok(output)
}

/// `namespace Foo {}` merges with a same-named class/function/etc. (TS
/// declaration merging), so when one of those exists the namespace symbol
/// and its CONTAINS edge are dropped, leaving one symbol per qualname
/// whichever came first.
fn dedup_namespace_symbols(output: &mut ExtractedFile) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut dup_ns: HashSet<String> = HashSet::new();
    for s in output.symbols.iter().filter(|s| s.kind != "namespace") {
        seen.insert(s.qualname.clone());
    }
    for s in output.symbols.iter().filter(|s| s.kind == "namespace") {
        if seen.contains(&s.qualname) {
            dup_ns.insert(s.qualname.clone());
        }
    }
    if dup_ns.is_empty() {
        return;
    }
    output
        .symbols
        .retain(|s| !(s.kind == "namespace" && dup_ns.contains(&s.qualname)));
    // The namespace's CONTAINS duplicates the merged symbol's own.
    let mut kept: HashSet<(Option<String>, Option<String>)> = HashSet::new();
    output.edges.retain(|e| {
        if e.kind != "CONTAINS"
            || !e
                .target_qualname
                .as_ref()
                .is_some_and(|t| dup_ns.contains(t))
        {
            return true;
        }
        kept.insert((e.source_qualname.clone(), e.target_qualname.clone()))
    });
}

/// Exports found in a file: the local names exported, whether the file is a
/// module at all (else it is a classic script whose top-level declarations
/// are globals), and whether some export form could not be analysed.
#[derive(Default)]
struct Exports {
    names: HashSet<String>,
    is_module: bool,
    opaque: bool,
}

/// Records every top-level symbol that is not exported (ESM `export`,
/// `export { a, b as c }`, CommonJS `module.exports`/`exports.x`) as
/// private, so the resolver's name-fallback tiers skip it for callers in
/// other files (issue #151). Same-file resolution is unaffected. Nothing is
/// marked for a classic script (no import/export/CommonJS at all) or for a
/// file with an export form we cannot fully analyse.
fn mark_unexported_private(
    root: Node<'_>,
    source: &str,
    module_name: &str,
    output: &mut ExtractedFile,
) {
    let mut exports = Exports::default();
    let mut cursor = root.walk();
    for stmt in root.named_children(&mut cursor) {
        collect_exported_names(stmt, source, &mut exports);
    }
    if !exports.is_module && !uses_require(root, source) {
        return;
    }
    if exports.opaque {
        return;
    }
    let unexported: Vec<String> = output
        .symbols
        .iter()
        .filter(|s| {
            s.kind != "module"
                && s.name != DEFAULT_EXPORT
                && s.qualname == build_qualname(module_name, &[], &s.name)
                && !exports.names.contains(&s.name)
        })
        .map(|s| s.qualname.clone())
        .collect();
    // Members of an unexported top-level symbol are unreachable from other
    // files too (issue #187).
    let members: Vec<String> = output
        .symbols
        .iter()
        .filter(|s| {
            unexported.iter().any(|q| {
                s.qualname
                    .strip_prefix(q)
                    .is_some_and(|r| r.starts_with('.'))
            })
        })
        .map(|s| s.qualname.clone())
        .collect();
    output.private_qualnames.extend(unexported);
    output.private_qualnames.extend(members);
}

/// Whether any `require(..)` call appears (marks a CommonJS module).
fn uses_require(root: Node<'_>, source: &str) -> bool {
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        if node.kind() == "call_expression"
            && node
                .child_by_field_name("function")
                .is_some_and(|f| f.kind() == "identifier" && node_text(f, source) == "require")
        {
            return true;
        }
        let mut cursor = node.walk();
        stack.extend(node.named_children(&mut cursor));
    }
    false
}

/// Names exported through an object literal (`{ a, b: c }`); a spread or
/// computed member makes the export opaque.
fn object_export_names(obj: Node<'_>, source: &str, ex: &mut Exports) {
    let mut cursor = obj.walk();
    for member in obj.named_children(&mut cursor) {
        match member.kind() {
            "shorthand_property_identifier" => {
                ex.names.insert(node_text(member, source));
            }
            "pair" => {
                if let Some(v) = member.child_by_field_name("value")
                    && v.kind() == "identifier"
                {
                    ex.names.insert(node_text(v, source));
                }
            }
            "method_definition" | "comment" => {}
            _ => ex.opaque = true,
        }
    }
}

/// Handles one exported value expression (`module.exports = <v>`,
/// `export default <v>`).
fn exported_value(v: Node<'_>, source: &str, ex: &mut Exports) {
    match v.kind() {
        "identifier" => {
            ex.names.insert(node_text(v, source));
        }
        "object" => object_export_names(v, source, ex),
        "function_expression"
        | "function"
        | "arrow_function"
        | "class"
        | "class_declaration"
        | "function_declaration"
        | "generator_function"
        | "string"
        | "number" => {}
        _ => ex.opaque = true,
    }
}

/// Local names a top-level statement exports, if it is an export at all.
fn collect_exported_names(stmt: Node<'_>, source: &str, ex: &mut Exports) {
    match stmt.kind() {
        "import_statement" | "import_declaration" => ex.is_module = true,
        "export_statement" | "export_declaration" => {
            ex.is_module = true;
            if let Some(decl) = stmt.child_by_field_name("declaration") {
                declared_names(decl, source, &mut ex.names);
            }
            if let Some(value) = stmt.child_by_field_name("value") {
                exported_value(value, source, ex);
            }
            // `export = foo`
            let mut cursor = stmt.walk();
            for child in stmt.named_children(&mut cursor) {
                if child.kind() == "identifier" {
                    ex.names.insert(node_text(child, source));
                }
            }
            // `export { a, b as c }` (a `from` re-export names no local symbol).
            if stmt.child_by_field_name("source").is_none() {
                for clause in stmt.named_children(&mut cursor) {
                    if clause.kind() != "export_clause" {
                        continue;
                    }
                    let mut c = clause.walk();
                    for spec in clause.named_children(&mut c) {
                        if let Some(name) = spec.child_by_field_name("name") {
                            ex.names.insert(node_text(name, source));
                        }
                    }
                }
            }
        }
        "expression_statement" => {
            let Some(expr) = stmt.named_child(0) else {
                return;
            };
            if expr.kind() == "call_expression" {
                // `Object.assign(module.exports, ..)` and the like.
                let text = node_text(expr, source);
                if text.contains("module.exports") || text.contains("exports.") {
                    ex.is_module = true;
                    ex.opaque = true;
                }
                return;
            }
            if expr.kind() != "assignment_expression" {
                return;
            }
            let (Some(left), Some(right)) = (
                expr.child_by_field_name("left"),
                expr.child_by_field_name("right"),
            ) else {
                return;
            };
            let left = node_text(left, source);
            if left == "module.exports"
                || left.starts_with("exports.")
                || left.starts_with("module.exports.")
            {
                ex.is_module = true;
                exported_value(right, source, ex);
            }
        }
        _ => {}
    }
}

/// Names bound by the declaration inside an `export` statement.
fn declared_names(decl: Node<'_>, source: &str, out: &mut HashSet<String>) {
    match decl.kind() {
        "lexical_declaration" | "variable_declaration" => {
            let mut cursor = decl.walk();
            for d in decl.named_children(&mut cursor) {
                if d.kind() == "variable_declarator"
                    && let Some(name) = d.child_by_field_name("name")
                {
                    let mut names = Vec::new();
                    collect_binding_names(name, source, &mut names);
                    out.extend(names);
                }
            }
        }
        "ambient_declaration" => {
            let mut cursor = decl.walk();
            for inner in decl.named_children(&mut cursor) {
                declared_names(inner, source, out);
            }
        }
        _ => {
            if let Some(name) = decl.child_by_field_name("name") {
                let text = node_text(name, source);
                // `namespace A.B {}` declares `A`.
                match (name.kind(), text.split_once('.')) {
                    ("nested_identifier", Some((head, _))) => out.insert(head.to_string()),
                    _ => out.insert(text),
                };
            }
        }
    }
}

fn collect_grpc_clients(root: Node<'_>, source: &str) -> HashMap<String, GrpcService> {
    let mut clients = HashMap::new();
    let mut stack = vec![root];
    while let Some(node) = stack.pop() {
        if node.kind() == "variable_declarator" {
            let Some(name_node) = node.child_by_field_name("name") else {
                continue;
            };
            if name_node.kind() != "identifier" {
                continue;
            }
            let Some(value_node) = node.child_by_field_name("value") else {
                continue;
            };
            let Some(service) = grpc_service_from_client_initializer(value_node, source) else {
                continue;
            };
            let name = node_text(name_node, source);
            if name.is_empty() {
                continue;
            }
            clients.insert(name, service);
        }
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            stack.push(child);
        }
    }
    clients
}

fn grpc_service_from_client_initializer(node: Node<'_>, source: &str) -> Option<GrpcService> {
    let mut current = node;
    loop {
        match current.kind() {
            "parenthesized_expression" => {
                current = current
                    .child_by_field_name("expression")
                    .or_else(|| current.named_child(0))?;
            }
            "await_expression" => {
                current = current
                    .child_by_field_name("argument")
                    .or_else(|| current.named_child(0))?;
            }
            "as_expression" | "type_assertion" | "non_null_expression" => {
                current = current
                    .child_by_field_name("expression")
                    .or_else(|| current.named_child(0))?;
            }
            _ => break,
        }
    }
    // Only `new Ctor(..)` declares a client (`new FooServiceClient(..)`,
    // `new proto.pkg.Greeter(..)`); a call result such as
    // `await jobScheduling.listJobs()` is data, not a client (#115).
    if current.kind() != "new_expression" {
        return None;
    }
    let target_node = call_target_node(current)?;
    let raw = node_text(target_node, source);
    grpc_service_from_path(&raw)
}

fn walk_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if (node.kind() == "jsx_element" || node.kind() == "jsx_self_closing_element")
        && let Some(edge) = jsx_route_edge(node, ctx, source)
    {
        output.edges.push(edge);
    }
    if (node.kind() == "jsx_element" || node.kind() == "jsx_self_closing_element")
        && let Some(edge) = jsx_component_call_edge(node, ctx, source)
    {
        output.edges.push(edge);
    }
    if node.kind() == "call_expression" || node.kind() == "new_expression" {
        // `handle_call` returns `true` when it has already fully walked a
        // callback argument itself with adjusted context (currently just
        // `fastify_register_walk`, which re-walks a `.register(cb, {
        // prefix })` callback with the accumulated route prefix folded
        // in). Recursing into this node's children generically afterwards
        // — now that an arrow-function argument is no longer a walk
        // boundary (see `is_lambda_node`) — would walk that same callback
        // body a second time with the *un*-prefixed `ctx`, duplicating its
        // edges under the wrong prefix. Returning here skips only the
        // generic recursion for *this* node; sibling calls are unaffected.
        if handle_call(node, ctx, source, output) {
            return;
        }
    }
    // const { DB_URL } = process.env (destructuring)
    if node.kind() == "variable_declarator" {
        for edge in process_env_destructuring_edges(node, ctx, source) {
            output.edges.push(edge);
        }
        // `export const X = (...) => {...}` and friends: a module-level
        // declarator `handle_variable_declaration` emitted a symbol for.
        // Walk its initializer with that symbol pending as the owner of
        // any function found inside (see `Context::fn_owner`). Only at
        // module (or namespace) scope: a handler const inside a component/function body
        // stays attributed to that component, like it does for a
        // `function` declaration. Destructuring (`const {a} = f()`) has no
        // single owner, so it's left alone.
        if ctx.class_stack.len() <= ctx.ns_depth
            && ctx.current_scope == container_qualname(&ctx.module, &ctx.class_stack)
            && let Some(name_node) = node.child_by_field_name("name")
            && name_node.kind() == "identifier"
            && let Some(value) = node.child_by_field_name("value")
        {
            let mut next_ctx = ctx.clone();
            next_ctx.fn_owner = Some(build_qualname(
                &ctx.module,
                &ctx.class_stack,
                &node_text(name_node, source),
            ));
            walk_node(value, &next_ctx, source, output);
            return;
        }
    }
    if let Some(next_ctx) = owned_function_scope(node, ctx, source) {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            walk_node(child, &next_ctx, source, output);
        }
        return;
    }
    // process.env.KEY (member_expression) or process.env["KEY"] (subscript_expression)
    if (node.kind() == "member_expression" || node.kind() == "optional_member_expression")
        && let Some(edge) = process_env_member_edge(node, ctx, source)
    {
        output.edges.push(edge);
    }
    if node.kind() == "subscript_expression"
        && let Some(edge) = process_env_subscript_edge(node, ctx, source)
    {
        output.edges.push(edge);
    }
    if is_dynamic_this_function_node(node.kind()) {
        return;
    }
    // An arrow function body is a nested *scope*, not a new symbol — fall
    // through into the generic recursion below with the same `ctx` so
    // calls inside it attribute to the enclosing named symbol instead of
    // being silently dropped (see `is_lambda_node`'s doc comment for why
    // this is safe only for arrow functions, not plain `function`
    // expressions). None of the match arms below fire for "arrow_function"
    // itself, so no special case is needed here beyond not returning early.
    match node.kind() {
        "class_declaration" | "abstract_class_declaration" => {
            handle_class(node, ctx, source, output);
            return;
        }
        "function_declaration" | "generator_function_declaration" => {
            if ctx.fn_depth > 0 {
                return;
            }
            handle_function(node, ctx, source, output);
            return;
        }
        "interface_declaration" => {
            handle_interface(node, ctx, source, output);
            return;
        }
        "internal_module" | "module" => {
            handle_namespace(node, ctx, source, output);
            return;
        }
        "type_alias_declaration" => {
            handle_named_item(node, ctx, source, output, "type");
            return;
        }
        "enum_declaration" => {
            handle_named_item(node, ctx, source, output, "enum");
            return;
        }
        "lexical_declaration" | "variable_declaration" => {
            handle_variable_declaration(node, ctx, source, output);
            // Don't return — fall through to recurse into children for call/config edges
        }
        "import_statement" | "import_declaration" => {
            handle_import(node, ctx, source, output, true);
            return;
        }
        "export_statement" | "export_declaration" => {
            handle_import(node, ctx, source, output, false);
            if handle_anonymous_default(node, ctx, source, output) {
                return;
            }
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

fn handle_class(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    handle_class_named(node, ctx, source, output, node_text(name_node, source));
}

fn handle_class_named(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    name: String,
) {
    if ctx.fn_depth > 0 || name.is_empty() {
        return;
    }
    let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    output.symbols.push(SymbolInput {
        kind: "class".to_string(),
        name: name.clone(),
        qualname: qualname.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature: None,
        docstring: None,
        identity: None,
    });
    let parent = container_qualname(&ctx.module, &ctx.class_stack);
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(parent),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    handle_class_heritage(node, &qualname, source, output);

    let mut next_ctx = ctx.clone();
    next_ctx.class_stack.push(name);
    next_ctx.current_scope = qualname.clone();
    if let Some(prefix) = controller_prefix_from_class(node, source) {
        next_ctx.route_prefix = Some(prefix);
    }
    if let Some(body) = node.child_by_field_name("body") {
        next_ctx.class_attr_types = Rc::new(collect_class_level_attr_types(body, source));
        walk_class_body(body, &next_ctx, source, output);
    }
}

fn handle_class_heritage(
    node: Node<'_>,
    class_qualname: &str,
    source: &str,
    output: &mut ExtractedFile,
) {
    let mut extends_targets = Vec::new();
    if let Some(super_node) = node.child_by_field_name("superclass") {
        let base = node_text(super_node, source);
        if !base.is_empty() {
            extends_targets.push(base);
        }
    }
    extends_targets.extend(collect_clause_targets_from(node, "extends_clause", source));
    let mut seen = std::collections::HashSet::new();
    for target in extends_targets {
        if target.is_empty() || !seen.insert(target.clone()) {
            continue;
        }
        output.edges.push(EdgeInput {
            kind: "EXTENDS".to_string(),
            source_qualname: Some(class_qualname.to_string()),
            target_qualname: Some(target),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }

    for target in collect_clause_targets_from(node, "implements_clause", source) {
        if target.is_empty() {
            continue;
        }
        output.edges.push(EdgeInput {
            kind: "IMPLEMENTS".to_string(),
            source_qualname: Some(class_qualname.to_string()),
            target_qualname: Some(target),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }
}

fn handle_interface(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let qualname = match handle_named_item(node, ctx, source, output, "interface") {
        Some(value) => value,
        None => return,
    };
    // tree-sitter-typescript names an interface's heritage `extends_type_clause`
    // (a class's is `extends_clause` inside `class_heritage`).
    for target in collect_clause_targets_from(node, "extends_type_clause", source) {
        if target.is_empty() {
            continue;
        }
        output.edges.push(EdgeInput {
            kind: "EXTENDS".to_string(),
            source_qualname: Some(qualname.clone()),
            target_qualname: Some(target),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }
}

fn collect_clause_targets_from(node: Node<'_>, clause_kind: &str, source: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            kind if kind == clause_kind => {
                targets.extend(clause_targets(child, source));
            }
            "class_heritage" | "heritage_clause" => {
                let mut saw_clause = false;
                let mut inner = child.walk();
                for clause in child.named_children(&mut inner) {
                    if clause.kind() == clause_kind {
                        targets.extend(clause_targets(clause, source));
                        saw_clause = true;
                    }
                }
                if !saw_clause
                    && clause_kind == "extends_clause"
                    && let Some(target) = class_heritage_target(child, source)
                {
                    targets.push(target);
                }
            }
            _ => {}
        }
    }
    targets
}

fn class_heritage_target(node: Node<'_>, source: &str) -> Option<String> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            "extends_clause" | "implements_clause" => continue,
            _ => {
                let name = node_text(child, source);
                if !name.is_empty() {
                    return Some(name);
                }
            }
        }
    }
    None
}

fn clause_targets(node: Node<'_>, source: &str) -> Vec<String> {
    let mut targets = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        let kind = child.kind();
        if kind == "type_arguments" || kind == "type_parameters" {
            continue;
        }
        // `B<T>` targets `B`.
        let name = if kind == "generic_type" {
            child
                .child_by_field_name("name")
                .map(|n| node_text(n, source))
                .unwrap_or_default()
        } else {
            node_text(child, source)
        };
        if !name.is_empty() {
            targets.push(name);
        }
    }
    targets
}

fn walk_class_body(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            "method_definition" | "abstract_method_signature" => {
                handle_method(child, ctx, source, output);
            }
            "public_field_definition" | "field_definition" => {
                handle_field(child, ctx, source, output);
            }
            _ => {}
        }
    }
}

fn push_field(
    node: Node<'_>,
    name: String,
    private: bool,
    ctx: &Context,
    output: &mut ExtractedFile,
) {
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    if private {
        output.private_qualnames.push(qualname.clone());
    }
    output.symbols.push(SymbolInput {
        kind: "field".to_string(),
        name,
        qualname: qualname.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature: None,
        docstring: None,
        identity: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(container_qualname(&ctx.module, &ctx.class_stack)),
        target_qualname: Some(qualname),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
}

/// A class property declaration (`readonly`, `?`, `static` and `#private`
/// included).
fn handle_field(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    // TS `public_field_definition` names it `name`; JS `field_definition`
    // `property`.
    let Some(name_node) = node
        .child_by_field_name("name")
        .or_else(|| node.child_by_field_name("property"))
    else {
        return;
    };
    // A computed key (`[Symbol.iterator] = ..`) has no stable name.
    if name_node.kind() == "computed_property_name" {
        return;
    }
    let name = node_text(name_node, source);
    push_field(node, name, is_private_member(node, source), ctx, output);
}

/// `constructor(private x: T, readonly y: U)`: each parameter carrying an
/// accessibility modifier or `readonly` also declares a field.
fn handle_parameter_properties(
    ctor: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let Some(params) = ctor.child_by_field_name("parameters") else {
        return;
    };
    let mut cursor = params.walk();
    for param in params.named_children(&mut cursor) {
        if !matches!(param.kind(), "required_parameter" | "optional_parameter") {
            continue;
        }
        let mut c = param.walk();
        let (mut is_property, mut private) = (false, false);
        for child in param.children(&mut c) {
            match child.kind() {
                "accessibility_modifier" => {
                    is_property = true;
                    private = node_text(child, source) == "private";
                }
                "readonly" => is_property = true,
                _ => {}
            }
        }
        let Some(pattern) = param.child_by_field_name("pattern") else {
            continue;
        };
        if is_property && pattern.kind() == "identifier" {
            push_field(param, node_text(pattern, source), private, ctx, output);
        }
    }
}

/// Returns `true` when `fastify_register_walk` already fully walked a
/// `.register(...)` callback argument itself (with the accumulated route
/// prefix folded into its context) — see that function's doc comment and
/// this function's call site in `walk_node` for why the caller must then
/// skip its own generic recursion into this node's children.
fn handle_call(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) -> bool {
    let register_handled = fastify_register_walk(node, ctx, source, output);
    for edge in http_route_edges(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = http_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    for edge in grpc_impl_edges(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = grpc_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = channel_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    let Some(target_node) = call_target_node(node) else {
        return register_handled;
    };
    let raw = node_text(target_node, source);
    if raw.is_empty() {
        return register_handled;
    }
    let receiver_type = infer_receiver_type(target_node, source, ctx);
    let target = resolve_call_target(&raw, ctx);
    let import_candidates = import_placeholder(&raw, ctx).into_iter().collect();
    let detail = if target.is_some() { None } else { Some(raw) };
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    output.edges.push(EdgeInput {
        kind: "CALLS".to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: target,
        detail,
        evidence_snippet: snippet,
        receiver_type,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        import_candidates,
        // A bare identifier callee (`foo()`) vs. anything qualified
        // (`this.foo()`, `obj.foo()`, ...) — see `EdgeInput::bare_call`'s
        // doc.
        bare_call: target_node.kind() == "identifier",
        ..Default::default()
    });
    register_handled
}

/// Collect `import` bindings declared at the top level of `root`.
fn collect_import_bindings(root: Node<'_>, source: &str) -> ImportBindings {
    let mut bindings = ImportBindings::new();
    let mut cursor = root.walk();
    for stmt in root.named_children(&mut cursor) {
        if stmt.kind() != "import_statement" {
            continue;
        }
        let Some(spec) = stmt
            .child_by_field_name("source")
            .and_then(|n| unquote_string_literal(&node_text(n, source)))
        else {
            continue;
        };
        let mut stmt_cursor = stmt.walk();
        for clause in stmt.named_children(&mut stmt_cursor) {
            if clause.kind() != "import_clause" {
                continue;
            }
            let mut clause_cursor = clause.walk();
            for part in clause.named_children(&mut clause_cursor) {
                match part.kind() {
                    "identifier" => {
                        let local = node_text(part, source);
                        bindings.insert(local, (spec.clone(), Some(DEFAULT_EXPORT.to_string())));
                    }
                    "namespace_import" => {
                        let mut ns_cursor = part.walk();
                        let local = part
                            .named_children(&mut ns_cursor)
                            .find(|n| n.kind() == "identifier");
                        if let Some(local) = local {
                            bindings.insert(node_text(local, source), (spec.clone(), None));
                        }
                    }
                    "named_imports" => {
                        let mut named_cursor = part.walk();
                        for item in part.named_children(&mut named_cursor) {
                            let Some(name) = item.child_by_field_name("name") else {
                                continue;
                            };
                            let name = node_text(name, source);
                            let name = unquote_string_literal(&name).unwrap_or(name);
                            let local = item
                                .child_by_field_name("alias")
                                .map(|n| node_text(n, source))
                                .unwrap_or_else(|| name.clone());
                            bindings.insert(local, (spec.clone(), Some(name)));
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    bindings
}

/// Placeholder import candidate (`{specifier}\0{member}`) for a call whose
/// root identifier is an import binding not shadowed by a local: `cn()` →
/// `cn`, `api.get()` (namespace) → `get`, `Foo.bar()` (named) → `Foo.bar`.
fn import_placeholder(raw: &str, ctx: &Context) -> Option<String> {
    let raw = collapse_call_target_whitespace(raw);
    if !is_simple_call_target(&raw) {
        return None;
    }
    let (root, rest) = match raw.split_once('.') {
        Some((root, rest)) => (root, Some(rest)),
        None => (raw.as_str(), None),
    };
    if ctx.local_types.contains_key(root) {
        return None;
    }
    let (spec, imported) = ctx.import_bindings.get(root)?;
    let member = match (imported, rest) {
        (Some(name), rest) if name == DEFAULT_EXPORT => {
            let tail = rest.map(|r| format!(".{r}")).unwrap_or_default();
            format!("{DEFAULT_IMPORT_MARK}{root}{tail}")
        }
        (Some(name), Some(rest)) => format!("{name}.{rest}"),
        (Some(name), None) => name.clone(),
        (None, Some(rest)) => rest.to_string(),
        (None, None) => return None,
    };
    Some(format!("{spec}{IMPORT_PLACEHOLDER_SEP}{member}"))
}

/// Detect process.env.KEY → CONFIG_READ
fn process_env_member_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let (receiver, property) = member_receiver_and_method(node, source)?;
    if receiver != "process.env" {
        return None;
    }
    // property is the env var name
    let env_uri = config::normalize_env_var_name(&property)?;
    let detail = config::build_config_read_detail("env", &env_uri, &property, "node");
    let (start_line, _, end_line, _, _, _) = span(node);
    Some(EdgeInput {
        kind: config::CONFIG_READ_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(env_uri),
        detail: Some(detail),
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

/// Detect process.env["KEY"] → CONFIG_READ
fn process_env_subscript_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let obj_node = node.child_by_field_name("object")?;
    let obj_text = node_text(obj_node, source);
    if obj_text != "process.env" {
        return None;
    }
    let index_node = node.child_by_field_name("index")?;
    let key = extract_string_literal(index_node, source)?;
    let env_uri = config::normalize_env_var_name(&key)?;
    let detail = config::build_config_read_detail("env", &env_uri, &key, "node");
    let (start_line, _, end_line, _, _, _) = span(node);
    Some(EdgeInput {
        kind: config::CONFIG_READ_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(env_uri),
        detail: Some(detail),
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

/// Detect `const { DB_URL, API_KEY } = process.env` → CONFIG_READ edges
fn process_env_destructuring_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    if node.kind() != "variable_declarator" {
        return edges;
    }
    let Some(value_node) = node.child_by_field_name("value") else {
        return edges;
    };
    if node_text(value_node, source) != "process.env" {
        return edges;
    }
    let Some(name_node) = node.child_by_field_name("name") else {
        return edges;
    };
    if name_node.kind() != "object_pattern" {
        return edges;
    }
    let (start_line, _, end_line, _, _, _) = span(node);
    let mut cursor = name_node.walk();
    for child in name_node.named_children(&mut cursor) {
        let env_name = match child.kind() {
            // const { DATABASE_URL } = process.env
            "shorthand_property_identifier_pattern" => node_text(child, source),
            // const { DB_URL: dbUrl } = process.env
            "pair_pattern" => {
                if let Some(key) = child.child_by_field_name("key") {
                    node_text(key, source)
                } else {
                    continue;
                }
            }
            _ => continue,
        };
        if env_name.is_empty() {
            continue;
        }
        let Some(env_uri) = config::normalize_env_var_name(&env_name) else {
            continue;
        };
        let detail = config::build_config_read_detail("env", &env_uri, &env_name, "node");
        edges.push(EdgeInput {
            kind: config::CONFIG_READ_KIND.to_string(),
            source_qualname: Some(ctx.current_scope.clone()),
            target_qualname: Some(env_uri),
            detail: Some(detail),
            evidence_start_line: Some(start_line),
            evidence_end_line: Some(end_line),
            ..Default::default()
        });
    }
    edges
}

fn http_route_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    if is_http_client_call(node, ctx, source) {
        return edges;
    }
    if let Some(edge) = express_direct_route_edge(node, ctx, source) {
        edges.push(edge);
    }
    if let Some(edge) = express_route_chain_edge(node, ctx, source) {
        edges.push(edge);
    }
    edges.extend(fastify_route_edges(node, ctx, source));
    edges
}

fn http_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    if let Some(edge) = fetch_call_edge(node, ctx, source) {
        return Some(edge);
    }
    axios_call_edge(node, ctx, source)
}

fn grpc_impl_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    if node.kind() != "call_expression" {
        return edges;
    }
    let Some(target_node) = call_target_node(node) else {
        return edges;
    };
    let Some((_receiver, method_name)) = member_receiver_and_method(target_node, source) else {
        return edges;
    };
    if method_name != "addService" {
        return edges;
    }
    let args = call_arguments(node);
    let Some(service_arg) = args.first() else {
        return edges;
    };
    let Some(service) = grpc_service_from_service_def(*service_arg, source) else {
        return edges;
    };
    let Some(handlers_arg) = args.get(1) else {
        return edges;
    };
    if handlers_arg.kind() != "object" {
        return edges;
    }
    for (rpc_name, handler) in grpc_handlers_from_object(*handlers_arg, ctx, source) {
        if let Some(edge) = grpc_impl_edge(node, &service, &rpc_name, handler, source) {
            edges.push(edge);
        }
    }
    edges
}

fn grpc_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    if node.kind() != "call_expression" {
        return None;
    }
    let target_node = call_target_node(node)?;
    let (object_node, method_name) = member_object_and_method(target_node, source)?;
    let method_name = unquote_string_literal(&method_name).unwrap_or(method_name);
    if GRPC_JS_RAW_METHODS.contains(&method_name.as_str()) {
        return grpc_call_edge_from_raw_path(node, ctx, source);
    }
    if GRPC_JS_SKIP_METHODS.contains(&method_name.as_str()) {
        return None;
    }
    let service = grpc_service_for_receiver(object_node, ctx, source)?;
    let (raw_path, normalized) =
        proto::normalize_rpc_path(service.package.as_deref(), &service.service, &method_name)?;
    let detail = json!({
        "framework": "grpc-js",
        "role": "client",
        "service": service.service,
        "rpc": method_name,
        "package": service.package.as_deref(),
        "raw": raw_path,
    })
    .to_string();
    Some(EdgeInput {
        kind: proto::RPC_CALL_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn grpc_call_edge_from_raw_path(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let args = call_arguments(node);
    let raw_path = args
        .first()
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let (service, rpc) = grpc_service_from_raw_path(&raw_path)?;
    let (raw_path, normalized) =
        proto::normalize_rpc_path(service.package.as_deref(), &service.service, &rpc)?;
    let detail = json!({
        "framework": "grpc-js",
        "role": "client",
        "service": service.service,
        "rpc": rpc,
        "package": service.package.as_deref(),
        "raw": raw_path,
    })
    .to_string();
    Some(EdgeInput {
        kind: proto::RPC_CALL_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn grpc_impl_edge(
    node: Node<'_>,
    service: &GrpcService,
    rpc_name: &str,
    handler: String,
    source: &str,
) -> Option<EdgeInput> {
    let (raw_path, normalized) =
        proto::normalize_rpc_path(service.package.as_deref(), &service.service, rpc_name)?;
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    let detail = json!({
        "framework": "grpc-js",
        "role": "server",
        "service": service.service.as_str(),
        "rpc": rpc_name,
        "package": service.package.as_deref(),
        "raw": raw_path,
    })
    .to_string();
    Some(EdgeInput {
        kind: proto::RPC_IMPL_KIND.to_string(),
        source_qualname: Some(handler),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

fn channel_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let target_node = call_target_node(node)?;
    let (receiver, method) = member_receiver_and_method(target_node, source)?;
    if !channel::is_bus_receiver(&receiver) {
        return None;
    }
    let kind = if channel::is_publish_method(&method) {
        channel::CHANNEL_PUBLISH_KIND
    } else if channel::is_subscribe_method(&method) {
        channel::CHANNEL_SUBSCRIBE_KIND
    } else {
        return None;
    };
    let args = call_arguments(node);
    let raw_topic = node_text(*args.first()?, source);
    let normalized = channel::resolve_topic(
        &raw_topic,
        &ctx.string_consts,
        &channel::LocalBinding::NotLocal,
    )?;
    let detail = if kind == channel::CHANNEL_PUBLISH_KIND {
        channel::build_publish_detail(&normalized, &raw_topic, "js-bus")
    } else {
        channel::build_subscribe_detail(&normalized, &raw_topic, "js-bus")
    };
    Some(EdgeInput {
        kind: kind.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn grpc_handlers_from_object(node: Node<'_>, ctx: &Context, source: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            "pair" => {
                let Some(key_node) = child.child_by_field_name("key") else {
                    continue;
                };
                let Some(rpc_name) = grpc_property_name(key_node, source) else {
                    continue;
                };
                let handler = child
                    .child_by_field_name("value")
                    .and_then(|node| handler_node_qualname(node, ctx, source))
                    .unwrap_or_else(|| ctx.current_scope.clone());
                out.push((rpc_name, handler));
            }
            "shorthand_property_identifier" | "shorthand_property_identifier_pattern" => {
                let rpc_name = node_text(child, source);
                if rpc_name.is_empty() {
                    continue;
                }
                let handler = resolve_call_target(&rpc_name, ctx)
                    .unwrap_or_else(|| ctx.current_scope.clone());
                out.push((rpc_name, handler));
            }
            "method_definition" => {
                let Some(name_node) = child.child_by_field_name("name") else {
                    continue;
                };
                let rpc_name = node_text(name_node, source);
                if rpc_name.is_empty() {
                    continue;
                }
                out.push((rpc_name, ctx.current_scope.clone()));
            }
            _ => {}
        }
    }
    out
}

fn grpc_property_name(node: Node<'_>, source: &str) -> Option<String> {
    let raw = node_text(node, source);
    if raw.is_empty() {
        return None;
    }
    if let Some(value) = unquote_string_literal(&raw) {
        return Some(value);
    }
    Some(raw.trim_matches('"').trim_matches('\'').to_string())
}

fn grpc_service_from_service_def(node: Node<'_>, source: &str) -> Option<GrpcService> {
    let raw = node_text(node, source);
    if raw.is_empty() {
        return None;
    }
    let mut trimmed = raw.trim();
    if let Some(stripped) = trimmed.strip_suffix(".service") {
        trimmed = stripped;
    }
    grpc_service_from_path(trimmed)
}

fn grpc_service_for_receiver(node: Node<'_>, ctx: &Context, source: &str) -> Option<GrpcService> {
    if node.kind() == "new_expression" {
        let constructor = call_target_node(node)?;
        let raw = node_text(constructor, source);
        return grpc_service_from_path(&raw);
    }
    let receiver = node_text(node, source);
    grpc_service_from_receiver(&receiver, ctx)
}

fn grpc_service_from_receiver(receiver: &str, ctx: &Context) -> Option<GrpcService> {
    let receiver = receiver.trim();
    if receiver.is_empty() {
        return None;
    }
    if let Some(service) = ctx.grpc_clients.get(receiver) {
        return Some(service.clone());
    }
    if let Some(last) = receiver.rsplit('.').next()
        && let Some(service) = ctx.grpc_clients.get(last)
    {
        return Some(service.clone());
    }
    // Only receivers registered from a `new` client constructor (#115).
    None
}

fn grpc_service_from_raw_path(raw_path: &str) -> Option<(GrpcService, String)> {
    let trimmed = raw_path.trim();
    if !trimmed.starts_with('/') {
        return None;
    }
    let trimmed = trimmed.trim_start_matches('/');
    let mut parts = trimmed.splitn(2, '/');
    let service_path = parts.next()?.trim();
    let rpc = parts.next()?.trim();
    if service_path.is_empty() || rpc.is_empty() {
        return None;
    }
    let service_parts: Vec<&str> = service_path
        .split('.')
        .filter(|part| !part.is_empty())
        .collect();
    let service = service_parts.last()?.trim();
    if service.is_empty() {
        return None;
    }
    let package = grpc_package_from_parts(&service_parts[..service_parts.len() - 1], false);
    Some((
        GrpcService {
            package,
            service: service.to_string(),
        },
        rpc.to_string(),
    ))
}

fn grpc_service_from_path(raw: &str) -> Option<GrpcService> {
    let trimmed = collapse_call_target_whitespace(raw);
    let trimmed = trimmed.as_str();
    if trimmed.is_empty() || !is_simple_call_target(trimmed) {
        return None;
    }
    let parts: Vec<&str> = trimmed.split('.').filter(|part| !part.is_empty()).collect();
    let service_token = parts.last()?.trim();
    if service_token.is_empty() {
        return None;
    }
    let (service, stripped) = strip_grpc_service_token(service_token);
    if service.is_empty() {
        return None;
    }
    if !stripped && !service.chars().any(|ch| ch.is_ascii_uppercase()) {
        return None;
    }
    let package = grpc_package_from_parts(&parts[..parts.len() - 1], true);
    Some(GrpcService { package, service })
}

fn strip_grpc_service_token(raw: &str) -> (String, bool) {
    let mut token = raw.trim();
    if let Some(idx) = token.find('<') {
        token = &token[..idx];
    }
    if let Some(idx) = token.find('(') {
        token = &token[..idx];
    }
    let token = token.trim();
    if token.is_empty() {
        return (String::new(), false);
    }
    let mut stripped = false;
    let mut stripped_client = false;
    let mut value = token;
    if let Some(base) = value.strip_suffix("Client")
        && !base.is_empty()
    {
        value = base;
        stripped = true;
        stripped_client = true;
    }
    if !stripped_client
        && let Some(base) = value.strip_suffix("Service")
        && !base.is_empty()
    {
        value = base;
        stripped = true;
    }
    (value.to_string(), stripped)
}

fn grpc_package_from_parts(parts: &[&str], drop_root: bool) -> Option<String> {
    if parts.is_empty() {
        return None;
    }
    let mut start = 0;
    if drop_root
        && let Some(first) = parts.first()
        && is_grpc_root_segment(first)
    {
        start = 1;
    }
    if start >= parts.len() {
        return None;
    }
    let mut package_parts = Vec::new();
    for part in &parts[start..] {
        let trimmed = part.trim();
        if !trimmed.is_empty() {
            package_parts.push(trimmed);
        }
    }
    if package_parts.is_empty() {
        None
    } else {
        Some(package_parts.join("."))
    }
}

fn is_grpc_root_segment(segment: &str) -> bool {
    let lower = segment.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "proto" | "root" | "pb" | "pkg" | "package" | "services" | "service"
    )
}

fn method_route_edges(
    node: Node<'_>,
    qualname: &str,
    ctx: &Context,
    source: &str,
) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    for decorator in decorator_nodes(node) {
        let Some((name, args)) = decorator_name_and_args(decorator, source) else {
            continue;
        };
        let method = match http::normalize_method(&name) {
            Some(method) => method,
            None => continue,
        };
        let raw = args
            .first()
            .and_then(|arg| extract_string_literal(*arg, source))
            .unwrap_or_else(|| "/".to_string());
        let prefix = ctx.route_prefix.as_deref().unwrap_or("/");
        let raw_path = http::join_paths(prefix, &raw);
        let normalized = match http::normalize_path(&raw_path) {
            Some(value) => value,
            None => continue,
        };
        let detail = http::build_route_detail(&method, &normalized, &raw_path, "nestjs");
        edges.push(EdgeInput {
            kind: http::HTTP_ROUTE_KIND.to_string(),
            source_qualname: Some(qualname.to_string()),
            target_qualname: Some(normalized),
            detail: Some(detail),
            evidence_snippet: None,
            evidence_start_line: Some(span(node).0),
            evidence_end_line: Some(span(node).2),
            ..Default::default()
        });
    }
    edges
}

fn controller_prefix_from_class(node: Node<'_>, source: &str) -> Option<String> {
    for decorator in decorator_nodes(node) {
        let Some((name, args)) = decorator_name_and_args(decorator, source) else {
            continue;
        };
        if name == "Controller" {
            let raw = args
                .first()
                .and_then(|arg| extract_string_literal(*arg, source))
                .unwrap_or_else(|| "/".to_string());
            return Some(raw);
        }
    }
    None
}

fn decorator_nodes(node: Node<'_>) -> Vec<Node<'_>> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "decorator" {
            out.push(child);
        }
    }
    out
}

fn decorator_name_and_args<'a>(node: Node<'a>, source: &str) -> Option<(String, Vec<Node<'a>>)> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "call_expression" {
            let Some(target_node) = call_target_node(child) else {
                continue;
            };
            let raw = node_text(target_node, source);
            let name = raw
                .split('.')
                .next_back()
                .unwrap_or(raw.as_str())
                .to_string();
            let args = call_arguments(child);
            return Some((name, args));
        }
    }
    let raw = node_text(node, source);
    let name = raw
        .trim_start_matches('@')
        .split('.')
        .next_back()
        .unwrap_or(raw.as_str())
        .to_string();
    if name.is_empty() {
        None
    } else {
        Some((name, Vec::new()))
    }
}

fn call_arguments(node: Node<'_>) -> Vec<Node<'_>> {
    let mut out = Vec::new();
    let Some(args) = node.child_by_field_name("arguments") else {
        return out;
    };
    let mut cursor = args.walk();
    for child in args.named_children(&mut cursor) {
        out.push(child);
    }
    out
}

fn extract_string_literal(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "template_string" {
        return None;
    }
    let raw = node_text(node, source);
    unquote_string_literal(&raw)
}

fn express_direct_route_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let target_node = call_target_node(node)?;
    let (receiver, method_name) = member_receiver_and_method(target_node, source)?;
    if !HTTP_METHOD_NAMES.contains(&method_name.as_str()) {
        return None;
    }
    if !is_router_receiver(&receiver, ctx) {
        return None;
    }
    let args = call_arguments(node);
    let raw_path = args
        .first()
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let prefix = ctx.route_prefix.as_deref().unwrap_or("/");
    let full_path = http::join_paths(prefix, &raw_path);
    // normalize_path rejects "/" (no alpha chars) — but for known route definitions
    // we accept any path starting with "/"
    let normalized = http::normalize_path(&full_path).or_else(|| {
        if full_path.starts_with('/') {
            Some(full_path.clone())
        } else {
            None
        }
    })?;
    let method = http::normalize_method(&method_name)?;
    let handler = handler_from_args(&args[1..], ctx, source);
    let framework = if receiver == "fastify" {
        "fastify"
    } else {
        "express"
    };
    let detail = http::build_route_detail(&method, &normalized, &full_path, framework);
    Some(EdgeInput {
        kind: http::HTTP_ROUTE_KIND.to_string(),
        source_qualname: Some(handler),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn express_route_chain_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let target_node = call_target_node(node)?;
    let (object_node, method_name) = member_object_and_method(target_node, source)?;
    if !HTTP_METHOD_NAMES.contains(&method_name.as_str()) {
        return None;
    }
    if object_node.kind() != "call_expression" {
        return None;
    }
    let route_call = object_node;
    let route_target = call_target_node(route_call)?;
    let (_route_receiver, route_method) = member_receiver_and_method(route_target, source)?;
    if route_method != "route" {
        return None;
    }
    let route_args = call_arguments(route_call);
    let raw_path = route_args
        .first()
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let normalized = http::normalize_path(&raw_path).or_else(|| {
        if raw_path.starts_with('/') {
            Some(raw_path.clone())
        } else {
            None
        }
    })?;
    let method = http::normalize_method(&method_name)?;
    let args = call_arguments(node);
    let handler = handler_from_args(&args, ctx, source);
    let detail = http::build_route_detail(&method, &normalized, &raw_path, "express");
    Some(EdgeInput {
        kind: http::HTTP_ROUTE_KIND.to_string(),
        source_qualname: Some(handler),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn fastify_route_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    let Some(target_node) = call_target_node(node) else {
        return edges;
    };
    let Some((receiver, method_name)) = member_receiver_and_method(target_node, source) else {
        return edges;
    };
    if method_name != "route" || !is_router_receiver(&receiver, ctx) {
        return edges;
    }
    let args = call_arguments(node);
    let Some(config) = args.first() else {
        return edges;
    };
    if config.kind() != "object" {
        return edges;
    }
    let raw_path = object_property_string(config, "url", source)
        .or_else(|| object_property_string(config, "path", source));
    let Some(raw_path) = raw_path else {
        return edges;
    };
    let prefix = ctx.route_prefix.as_deref().unwrap_or("/");
    let full_path = http::join_paths(prefix, &raw_path);
    let normalized = match http::normalize_path(&full_path).or_else(|| {
        if full_path.starts_with('/') {
            Some(full_path.clone())
        } else {
            None
        }
    }) {
        Some(value) => value,
        None => return edges,
    };
    let handler = object_property_node(config, "handler", source)
        .and_then(|node| handler_node_qualname(node, ctx, source));
    let handler = handler.unwrap_or_else(|| ctx.current_scope.clone());
    let methods = object_property_methods(config, source);
    for method in methods {
        let detail = http::build_route_detail(&method, &normalized, &full_path, "fastify");
        edges.push(EdgeInput {
            kind: http::HTTP_ROUTE_KIND.to_string(),
            source_qualname: Some(handler.clone()),
            target_qualname: Some(normalized.clone()),
            detail: Some(detail),
            evidence_snippet: None,
            evidence_start_line: Some(span(node).0),
            evidence_end_line: Some(span(node).2),
            ..Default::default()
        });
    }
    edges
}

/// Walk up to the root (program) node.
fn root_node(node: Node<'_>) -> Node<'_> {
    let mut n = node;
    while let Some(p) = n.parent() {
        n = p;
    }
    n
}

/// Find a top-level declaration's value by name.
/// Returns the function_declaration itself, or the value node of a variable_declarator.
fn find_declaration_value<'a>(root: Node<'a>, name: &str, source: &str) -> Option<Node<'a>> {
    let mut cursor = root.walk();
    for child in root.named_children(&mut cursor) {
        match child.kind() {
            "function_declaration" | "generator_function_declaration" => {
                if let Some(n) = child.child_by_field_name("name")
                    && node_text(n, source) == name
                {
                    return Some(child);
                }
            }
            "lexical_declaration" | "variable_declaration" => {
                let mut c = child.walk();
                for decl in child.named_children(&mut c) {
                    if decl.kind() == "variable_declarator"
                        && let Some(n) = decl.child_by_field_name("name")
                        && node_text(n, source) == name
                    {
                        return decl.child_by_field_name("value");
                    }
                }
            }
            "export_statement" | "export_declaration" => {
                if let Some(found) = find_declaration_value(child, name, source) {
                    return Some(found);
                }
            }
            _ => {}
        }
    }
    None
}

/// Resolve a node to a function node, following identifiers and unwrapping
/// call wrappers like `fp(callback)` or `fastifyPlugin(callback)`.
fn resolve_to_function<'a>(
    node: Node<'a>,
    root: Node<'a>,
    source: &str,
    depth: usize,
) -> Option<Node<'a>> {
    if depth > 3 {
        return None;
    }
    match node.kind() {
        "arrow_function"
        | "function_expression"
        | "function"
        | "function_declaration"
        | "generator_function_declaration" => Some(node),
        "identifier" => {
            let name = node_text(node, source);
            let value = find_declaration_value(root, &name, source)?;
            resolve_to_function(value, root, source, depth + 1)
        }
        "call_expression" => {
            // Unwrap wrappers like fp(callback), fastifyPlugin(callback)
            let args = call_arguments(node);
            for arg in args {
                if let Some(f) = resolve_to_function(arg, root, source, depth + 1) {
                    return Some(f);
                }
            }
            None
        }
        _ => None,
    }
}

/// Extract the name of a function's first parameter, handling TypeScript typed params.
fn first_param_name(func: Node<'_>, source: &str) -> Option<String> {
    if let Some(params) = func.child_by_field_name("parameters") {
        let mut cursor = params.walk();
        if let Some(first) = params.named_children(&mut cursor).next() {
            // TypeScript typed params: required_parameter { pattern: identifier }
            let name_node = first.child_by_field_name("pattern").unwrap_or(first);
            let name = node_text(name_node, source);
            if !name.is_empty() {
                return Some(name);
            }
        }
    } else if let Some(param) = func.child_by_field_name("parameter") {
        // Arrow functions with single unparenthesized param
        let name = node_text(param, source);
        if !name.is_empty() {
            return Some(name);
        }
    }
    None
}

fn fastify_register_walk(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) -> bool {
    let Some(target_node) = call_target_node(node) else {
        return false;
    };
    let Some((receiver, method_name)) = member_receiver_and_method(target_node, source) else {
        return false;
    };
    if method_name != "register" || !is_router_receiver(&receiver, ctx) {
        return false;
    }
    let args = call_arguments(node);
    let root = root_node(node);
    let callback = args
        .iter()
        .find_map(|a| resolve_to_function(*a, root, source, 0));
    let Some(callback) = callback else {
        return false;
    };
    let prefix_str = args.iter().find_map(|a| {
        if a.kind() == "object" {
            object_property_string(a, "prefix", source)
        } else {
            None
        }
    });
    let mut next_ctx = ctx.clone();
    if let Some(prefix) = prefix_str {
        let existing = ctx.route_prefix.as_deref().unwrap_or("/");
        next_ctx.route_prefix = Some(http::join_paths(existing, &prefix));
    }
    // The callback's first parameter is the Fastify instance — treat it as a router receiver
    if let Some(name) = first_param_name(callback, source)
        && !is_router_receiver_static(&name)
    {
        next_ctx.router_aliases.push(name);
    }
    // For function_declarations, handle_function already walked the body and may have
    // emitted HTTP_ROUTE edges (without prefix). Remove those — we'll re-emit with the
    // correct prefix from the register context.
    if matches!(
        callback.kind(),
        "function_declaration" | "generator_function_declaration"
    ) && let Some(name_node) = callback.child_by_field_name("name")
    {
        let func_name = node_text(name_node, source);
        if !func_name.is_empty() {
            let func_qualname = build_qualname(&ctx.module, &ctx.class_stack, &func_name);
            output.edges.retain(|e| {
                !(e.kind == http::HTTP_ROUTE_KIND
                    && e.source_qualname.as_deref() == Some(&func_qualname))
            });
        }
    }
    let body = callback.child_by_field_name("body");
    if let Some(body) = body {
        let mut cursor = body.walk();
        for child in body.named_children(&mut cursor) {
            walk_node(child, &next_ctx, source, output);
        }
    }
    true
}

fn fetch_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let target_node = call_target_node(node)?;
    if !is_fetch_callee(target_node, source) {
        return None;
    }
    let args = call_arguments(node);
    let raw_path = args
        .first()
        .and_then(|arg| http_url_argument(*arg, source))?;
    let normalized = http::normalize_path(&raw_path)?;
    let method = args
        .get(1)
        .and_then(|arg| object_property_string(arg, "method", source))
        .and_then(|raw| http::normalize_method(&raw))
        .unwrap_or_else(|| "GET".to_string());
    let detail = http::build_call_detail(&method, &normalized, &raw_path, "fetch");
    Some(EdgeInput {
        kind: http::HTTP_CALL_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn axios_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let target_node = call_target_node(node)?;
    let args = call_arguments(node);
    let build = |raw_path: String, base: Option<&String>, method: String| -> Option<EdgeInput> {
        let full = match base {
            Some(base) if !raw_path.contains("://") => http::join_paths(base, &raw_path),
            _ => raw_path.clone(),
        };
        let normalized = http::normalize_path(&full)?;
        let detail = http::build_call_detail(&method, &normalized, &raw_path, "axios");
        Some(EdgeInput {
            kind: http::HTTP_CALL_KIND.to_string(),
            source_qualname: Some(ctx.current_scope.clone()),
            target_qualname: Some(normalized),
            detail: Some(detail),
            evidence_snippet: None,
            evidence_start_line: Some(span(node).0),
            evidence_end_line: Some(span(node).2),
            ..Default::default()
        })
    };
    let config_method = |config: &Node<'_>| {
        object_property_string(config, "method", source)
            .and_then(|raw| http::normalize_method(&raw))
            .unwrap_or_else(|| "GET".to_string())
    };
    let instance = axios_instance_of(target_node, ctx, source);
    let base = instance.and_then(|b| b.as_ref());
    if is_axios_identifier(target_node, source)
        || (target_node.kind() == "identifier" && instance.is_some())
    {
        let config = args.first()?;
        let raw_path = object_property_url(config, "url", source)?;
        return build(raw_path, base, config_method(config));
    }
    let (receiver, method_name) = member_receiver_and_method(target_node, source)?;
    if receiver != "axios" && instance.is_none() {
        return None;
    }
    if instance.is_some() && method_name == "request" {
        let config = args.first()?;
        let raw_path = object_property_url(config, "url", source)?;
        return build(raw_path, base, config_method(config));
    }
    if !HTTP_METHOD_NAMES.contains(&method_name.as_str()) {
        return None;
    }
    let raw_path = args
        .first()
        .and_then(|arg| http_url_argument(*arg, source))?;
    build(raw_path, base, http::normalize_method(&method_name)?)
}

fn is_http_client_call(node: Node<'_>, ctx: &Context, source: &str) -> bool {
    let Some(target_node) = call_target_node(node) else {
        return false;
    };
    is_fetch_callee(target_node, source)
        || is_axios_callee(target_node, source)
        || axios_instance_of(target_node, ctx, source).is_some()
}

/// The `axios.create` instance a callee (`api(...)` or `api.get`) refers to,
/// with its static base path.
fn axios_instance_of<'c>(
    node: Node<'_>,
    ctx: &'c Context,
    source: &str,
) -> Option<&'c Option<String>> {
    if node.kind() == "identifier" {
        return ctx.axios_instances.get(&node_text(node, source));
    }
    let (receiver, _) = member_receiver_and_method(node, source)?;
    ctx.axios_instances.get(&receiver)
}

/// Every `const|let|var name = axios.create({...})` in the file.
fn collect_axios_instances(root: Node<'_>, source: &str) -> HashMap<String, Option<String>> {
    fn walk(node: Node<'_>, source: &str, out: &mut HashMap<String, Option<String>>) {
        if node.kind() == "variable_declarator"
            && let (Some(name), Some(value)) = (
                node.child_by_field_name("name"),
                node.child_by_field_name("value"),
            )
            && name.kind() == "identifier"
            && value.kind() == "call_expression"
            && let Some(callee) = call_target_node(value)
            && member_receiver_and_method(callee, source)
                .is_some_and(|(recv, method)| recv == "axios" && method == "create")
        {
            let base = call_arguments(value)
                .first()
                .and_then(|config| object_property_url(config, "baseURL", source))
                .and_then(|raw| http::normalize_path(&raw));
            out.insert(node_text(name, source), base);
        }
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            walk(child, source, out);
        }
    }
    let mut out = HashMap::new();
    walk(root, source, &mut out);
    out
}

fn is_fetch_callee(node: Node<'_>, source: &str) -> bool {
    if is_identifier_named(node, source, "fetch") {
        return true;
    }
    let Some((receiver, method)) = member_receiver_and_method(node, source) else {
        return false;
    };
    method == "fetch" && (receiver == "window" || receiver == "global" || receiver == "globalThis")
}

fn is_axios_callee(node: Node<'_>, source: &str) -> bool {
    if is_axios_identifier(node, source) {
        return true;
    }
    let Some((receiver, _method)) = member_receiver_and_method(node, source) else {
        return false;
    };
    receiver == "axios"
}

fn is_axios_identifier(node: Node<'_>, source: &str) -> bool {
    is_identifier_named(node, source, "axios")
}

fn is_identifier_named(node: Node<'_>, source: &str, name: &str) -> bool {
    node.kind() == "identifier" && node_text(node, source) == name
}

fn member_receiver_and_method(node: Node<'_>, source: &str) -> Option<(String, String)> {
    if node.kind() != "member_expression" && node.kind() != "optional_member_expression" {
        return None;
    }
    let receiver = node
        .child_by_field_name("object")
        .map(|obj| node_text(obj, source))?;
    let method = node
        .child_by_field_name("property")
        .map(|prop| node_text(prop, source))?;
    Some((receiver, method))
}

fn member_object_and_method<'a>(node: Node<'a>, source: &str) -> Option<(Node<'a>, String)> {
    if node.kind() != "member_expression" && node.kind() != "optional_member_expression" {
        return None;
    }
    let object = node.child_by_field_name("object")?;
    let method = node
        .child_by_field_name("property")
        .map(|prop| node_text(prop, source))?;
    Some((object, method))
}

fn is_router_receiver_static(raw: &str) -> bool {
    let head = raw.split('.').next().unwrap_or(raw);
    ROUTER_RECEIVERS.contains(&head)
}

fn is_router_receiver(raw: &str, ctx: &Context) -> bool {
    let head = raw.split('.').next().unwrap_or(raw);
    ROUTER_RECEIVERS.contains(&head) || ctx.router_aliases.iter().any(|a| a == head)
}

fn handler_from_args(args: &[Node<'_>], ctx: &Context, source: &str) -> String {
    if let Some(last) = args.last()
        && let Some(name) = handler_node_qualname(*last, ctx, source)
    {
        return name;
    }
    ctx.current_scope.clone()
}

fn handler_node_qualname(node: Node<'_>, ctx: &Context, source: &str) -> Option<String> {
    match node.kind() {
        "identifier"
        | "member_expression"
        | "optional_member_expression"
        | "shorthand_property_identifier"
        | "shorthand_property_identifier_pattern" => {
            let raw = node_text(node, source);
            resolve_call_target(&raw, ctx)
        }
        _ => None,
    }
}

fn object_property_node<'a>(node: &'a Node<'a>, key: &str, source: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "pair" {
            continue;
        }
        let Some(key_node) = child.child_by_field_name("key") else {
            continue;
        };
        let key_text = node_text(key_node, source);
        let key_text = key_text.trim_matches('"').trim_matches('\'');
        if key_text != key {
            continue;
        }
        let value = child.child_by_field_name("value")?;
        return Some(value);
    }
    None
}

fn object_property_string(node: &Node<'_>, key: &str, source: &str) -> Option<String> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "pair" {
            continue;
        }
        let Some(key_node) = child.child_by_field_name("key") else {
            continue;
        };
        let key_text = node_text(key_node, source);
        let key_text = key_text.trim_matches('"').trim_matches('\'');
        if key_text != key {
            continue;
        }
        let Some(value_node) = child.child_by_field_name("value") else {
            continue;
        };
        if let Some(value) = extract_string_literal(value_node, source) {
            return Some(value);
        }
    }
    None
}

/// Like `object_property_string`, but a template-literal value yields its
/// static URL path (see `template_url_path`).
fn object_property_url(node: &Node<'_>, key: &str, source: &str) -> Option<String> {
    let value = object_property_node(node, key, source)?;
    http_url_argument(value, source)
}

fn object_property_methods(node: &Node<'_>, source: &str) -> Vec<String> {
    let mut methods = Vec::new();
    let Some(value_node) = object_property_node(node, "method", source) else {
        return vec![http::HTTP_ANY.to_string()];
    };
    match value_node.kind() {
        "array" => {
            let mut cursor = value_node.walk();
            for child in value_node.named_children(&mut cursor) {
                if let Some(raw) = extract_string_literal(child, source)
                    && let Some(method) = http::normalize_method(&raw)
                {
                    methods.push(method);
                }
            }
        }
        _ => {
            if let Some(raw) = extract_string_literal(value_node, source)
                && let Some(method) = http::normalize_method(&raw)
            {
                methods.push(method);
            }
        }
    }
    if methods.is_empty() {
        methods.push(http::HTTP_ANY.to_string());
    }
    methods
}

/// Returns the opening tag node for a JSX element or self-closing element.
/// A `jsx_element` node's opening tag is its `open_tag` field (see
/// both grammars' `node-types.json`) — a self-closing element has no
/// separate opening tag node, it *is* the opening tag.
fn jsx_opening_tag(node: Node<'_>) -> Option<Node<'_>> {
    match node.kind() {
        "jsx_element" => node.child_by_field_name("open_tag"),
        "jsx_self_closing_element" => Some(node),
        _ => None,
    }
}

fn jsx_route_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let opening = jsx_opening_tag(node)?;
    let name_node = opening.child_by_field_name("name")?;
    let name = node_text(name_node, source);
    if name != "Route" {
        return None;
    }
    let mut raw_path = None;
    let mut cursor = opening.walk();
    for child in opening.named_children(&mut cursor) {
        if child.kind() != "jsx_attribute" {
            continue;
        }
        let Some(attr_name) = child.child_by_field_name("name") else {
            continue;
        };
        let attr_name = node_text(attr_name, source);
        if attr_name != "path" {
            continue;
        }
        if let Some(value_node) = child.child_by_field_name("value") {
            raw_path = extract_string_literal(value_node, source);
        }
    }
    let raw_path = raw_path?;
    let normalized = http::normalize_path(&raw_path)?;
    Some(EdgeInput {
        kind: http::PAGE_ROUTE_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized.clone()),
        detail: Some(json!({"framework":"react_router","path":normalized}).to_string()),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

/// `<Foo />` / `<Foo ...>...</Foo>` / `<ns.Foo>` — a capitalized JSX tag
/// name is a reference to an in-scope component, never a literal DOM tag
/// string (React's own convention: a lowercase name always compiles to a
/// string, an uppercase or dotted one always compiles to the referenced
/// value — see https://react.dev/learn/your-first-component). Before this,
/// `walk_node` only fed a `jsx_element`/`jsx_self_closing_element` node to
/// `jsx_route_edge` (react-router `<Route>` detection only), so every other
/// JSX usage was invisible to the graph and every component came back with
/// 0 callers (issue #111). Emits a CALLS edge through the same
/// `resolve_call_target`/`import_placeholder` path `handle_call` uses for
/// an ordinary call, so a rendered component resolves through imports
/// exactly like a function call does. Lowercase intrinsic tags (`<div>`)
/// and the `<ns:Foo>` XML-namespace form return `None`.
fn jsx_component_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let opening = jsx_opening_tag(node)?;
    let name_node = opening.child_by_field_name("name")?;
    // Only a bare identifier (`Foo`) or a dotted member access
    // (`ns.Foo`, aliased by the grammar to `member_expression`) is a
    // component reference; `jsx_namespace_name` (`<svg:rect>`) is a
    // literal namespaced tag, not an expression.
    if name_node.kind() != "identifier" && name_node.kind() != "member_expression" {
        return None;
    }
    let raw = node_text(name_node, source);
    let last_segment = raw.rsplit('.').next().unwrap_or(raw.as_str());
    if !last_segment
        .chars()
        .next()
        .is_some_and(|ch| ch.is_uppercase())
    {
        return None;
    }
    let target = resolve_call_target(&raw, ctx);
    let import_candidates = import_placeholder(&raw, ctx).into_iter().collect();
    let detail = if target.is_some() { None } else { Some(raw) };
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    Some(EdgeInput {
        kind: "CALLS".to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: target,
        detail,
        evidence_snippet: snippet,
        receiver_type: infer_receiver_type(name_node, source, ctx),
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        import_candidates,
        bare_call: name_node.kind() == "identifier",
        ..Default::default()
    })
}

fn next_page_route_edge(module_name: &str) -> Option<EdgeInput> {
    let route = NextRoute::locate(module_name)?;
    if route.router == NextRouter::App && route.leaf() != Some("page") {
        return None;
    }
    let raw = route.url_path(false)?;
    let normalized = http::normalize_path(&raw)?;
    Some(EdgeInput {
        kind: http::PAGE_ROUTE_KIND.to_string(),
        source_qualname: Some(module_name.to_string()),
        target_qualname: Some(normalized.clone()),
        detail: Some(json!({"framework": "nextjs", "path": normalized}).to_string()),
        evidence_snippet: None,
        evidence_start_line: None,
        evidence_end_line: None,
        ..Default::default()
    })
}

/// HTTP_ROUTE edges for Next.js API endpoints: one per exported HTTP-method
/// handler for App Router `route.*` files (HTTP_ANY when none are found), and a
/// single HTTP_ANY edge for Pages Router `pages/api/**`.
fn next_api_route_edges(module_name: &str, root: Node<'_>, source: &str) -> Vec<EdgeInput> {
    let Some(route) = NextRoute::locate(module_name) else {
        return Vec::new();
    };
    let methods = match route.router {
        NextRouter::App => {
            if route.leaf() != Some("route") {
                return Vec::new();
            }
            let mut methods = next_exported_methods(root, source);
            if methods.is_empty() {
                methods.push(http::HTTP_ANY.to_string());
            }
            methods
        }
        NextRouter::Pages => {
            if route.dirs.first() != Some(&"api") {
                return Vec::new();
            }
            vec![http::HTTP_ANY.to_string()]
        }
    };
    let Some(raw) = route.url_path(true) else {
        return Vec::new();
    };
    // normalize_path rejects "/" (no alpha chars), but `app/route.ts` is a real route.
    let Some(normalized) = http::normalize_path(&raw).or_else(|| (raw == "/").then(|| raw.clone()))
    else {
        return Vec::new();
    };
    methods
        .into_iter()
        .map(|method| EdgeInput {
            kind: http::HTTP_ROUTE_KIND.to_string(),
            source_qualname: Some(module_name.to_string()),
            target_qualname: Some(normalized.clone()),
            detail: Some(http::build_route_detail(
                &method,
                &normalized,
                &raw,
                "nextjs",
            )),
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            ..Default::default()
        })
        .collect()
}

/// HTTP method names exported from a route module: `export function GET`,
/// `export const GET = ...`, `export { handler as GET }`.
fn next_exported_methods(root: Node<'_>, source: &str) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let mut cursor = root.walk();
    for stmt in root.named_children(&mut cursor) {
        if stmt.kind() != "export_statement" {
            continue;
        }
        if let Some(decl) = stmt.child_by_field_name("declaration") {
            match decl.kind() {
                "function_declaration" | "generator_function_declaration" => {
                    if let Some(name) = decl.child_by_field_name("name") {
                        names.push(node_text(name, source));
                    }
                }
                "lexical_declaration" | "variable_declaration" => {
                    let mut dc = decl.walk();
                    for declarator in decl.named_children(&mut dc) {
                        if let Some(name) = declarator.child_by_field_name("name")
                            && name.kind() == "identifier"
                        {
                            names.push(node_text(name, source));
                        }
                    }
                }
                _ => {}
            }
            continue;
        }
        let mut sc = stmt.walk();
        for child in stmt.named_children(&mut sc) {
            if child.kind() != "export_clause" {
                continue;
            }
            let mut ec = child.walk();
            for spec in child.named_children(&mut ec) {
                if let Some(name) = spec
                    .child_by_field_name("alias")
                    .or_else(|| spec.child_by_field_name("name"))
                {
                    names.push(node_text(name, source));
                }
            }
        }
    }
    let mut methods: Vec<String> = Vec::new();
    for name in names {
        // App Router handlers are exported in upper case.
        if http::normalize_method(&name).as_deref() == Some(name.as_str())
            && name != http::HTTP_ANY
            && !methods.contains(&name)
        {
            methods.push(name);
        }
    }
    methods
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NextRouter {
    App,
    Pages,
}

/// A module inside a Next.js `app/` or `pages/` tree. `dirs` are the path
/// segments below the router root, including the leaf (`page` / `route` for
/// the App Router).
struct NextRoute<'a> {
    router: NextRouter,
    dirs: Vec<&'a str>,
}

impl<'a> NextRoute<'a> {
    /// Locates the router root inside a module path such as
    /// `web/src/app/api/tables/route`. A root segment counts only when it is
    /// first, directly under `src`, or within two levels of the repo root with
    /// no `src` above it, so `src/features/pages/list` is not a route.
    fn locate(module_name: &'a str) -> Option<Self> {
        let parts: Vec<&str> = module_name.split('/').collect();
        let is_root =
            |i: usize| i == 0 || parts[i - 1] == "src" || (i <= 2 && !parts[..i].contains(&"src"));
        let find = |name: &str| {
            (0..parts.len())
                .rev()
                .find(|&i| parts[i] == name && is_root(i))
        };
        if matches!(parts.last(), Some(&"page") | Some(&"route"))
            && let Some(idx) = find("app").filter(|idx| idx + 1 < parts.len())
        {
            return Some(Self {
                router: NextRouter::App,
                dirs: parts[idx + 1..].to_vec(),
            });
        }
        let idx = find("pages")?;
        // `pages/index.tsx` has module name `pages`; only accept that shape at a
        // conventional root so `utils/pages.ts` is not read as the index route.
        if idx + 1 == parts.len() && idx != 0 && parts[idx - 1] != "src" {
            return None;
        }
        let dirs = parts[idx + 1..].to_vec();
        if dirs.last().is_some_and(|leaf| leaf.starts_with('_')) {
            return None;
        }
        Some(Self {
            router: NextRouter::Pages,
            dirs,
        })
    }

    fn leaf(&self) -> Option<&'a str> {
        self.dirs.last().copied()
    }

    /// The served URL path. Route groups `(x)` and parallel slots `@x` do not
    /// appear in the URL; `_private` App Router folders are not routable.
    /// Non-API routes never live under `api`.
    fn url_path(&self, api: bool) -> Option<String> {
        let mut segments = self.dirs.clone();
        if self.router == NextRouter::App {
            segments.pop();
        }
        if !api && segments.first() == Some(&"api") {
            return None;
        }
        let mut out = String::new();
        for seg in segments {
            if seg.is_empty() || seg.starts_with('(') || seg.starts_with('@') || seg == "index" {
                continue;
            }
            if self.router == NextRouter::App && seg.starts_with('_') {
                return None;
            }
            out.push('/');
            if seg.starts_with('[') && seg.ends_with(']') {
                out.push(':');
                out.push_str(
                    seg.trim_start_matches('[')
                        .trim_end_matches(']')
                        .trim_start_matches("..."),
                );
            } else {
                out.push_str(seg);
            }
        }
        if out.is_empty() {
            out.push('/');
        }
        Some(out)
    }
}

/// URL argument of an HTTP client call: a plain string literal, or the static
/// path of a template literal.
fn http_url_argument(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "template_string" {
        return template_url_path(&node_text(node, source));
    }
    extract_string_literal(node, source)
}

enum TemplatePiece {
    Text(String),
    Substitution,
}

/// Static path of a template-literal URL such as `${BASE}/api/t/${id}?x=${y}`.
/// A leading `${...}` base URL is dropped, substitutions between path
/// separators become param segments, a substitution glued to path text ends
/// the path, and `?query` / `#fragment` are cut. `None` without a static path.
fn template_url_path(text: &str) -> Option<String> {
    use TemplatePiece::{Substitution, Text};
    let inner = text.trim().strip_prefix('`')?.strip_suffix('`')?;
    let mut pieces: Vec<TemplatePiece> = Vec::new();
    let mut rest = inner;
    while !rest.is_empty() {
        let Some(start) = rest.find("${") else {
            pieces.push(Text(rest.to_string()));
            break;
        };
        if start > 0 {
            pieces.push(Text(rest[..start].to_string()));
        }
        let mut depth = 0usize;
        let mut end = None;
        for (i, ch) in rest[start + 2..].char_indices() {
            match ch {
                '{' => depth += 1,
                '}' if depth == 0 => {
                    end = Some(start + 2 + i + 1);
                    break;
                }
                '}' => depth -= 1,
                _ => {}
            }
        }
        pieces.push(Substitution);
        rest = &rest[end?..];
    }
    let mut iter = pieces.into_iter().peekable();
    let mut out = String::new();
    // Drop a leading base URL substitution, or `https://${host}`.
    match iter.peek() {
        Some(Substitution) => {
            iter.next();
        }
        Some(Text(first)) if first.ends_with("://") => {
            iter.next();
            if matches!(iter.peek(), Some(Substitution)) {
                iter.next();
                // Host remainder, e.g. `:3000/api/x`; keep from the first slash.
                match iter.next() {
                    Some(Text(text)) => out.push_str(&text[text.find('/')?..]),
                    _ => return None,
                }
            } else {
                return None;
            }
        }
        _ => {}
    }
    if out.is_empty() {
        match iter.peek() {
            Some(Text(text)) if text.starts_with('/') || text.contains("://") => {}
            _ => return None,
        }
    }
    let mut prev_text = false;
    for piece in iter {
        match piece {
            Substitution => {
                if prev_text && !out.ends_with('/') {
                    break;
                }
                out.push_str("${}");
                prev_text = false;
            }
            Text(text) => {
                prev_text = true;
                if let Some(cut) = text.find(['?', '#']) {
                    out.push_str(&text[..cut]);
                    break;
                }
                out.push_str(&text);
            }
        }
    }
    http::normalize_path(&out)
}

fn call_target_node(node: Node<'_>) -> Option<Node<'_>> {
    node.child_by_field_name("function")
        .or_else(|| node.child_by_field_name("callee"))
        .or_else(|| node.child_by_field_name("constructor"))
}

fn resolve_call_target(raw: &str, ctx: &Context) -> Option<String> {
    let raw = collapse_call_target_whitespace(raw);
    let raw = raw.as_str();
    if raw.is_empty() || !is_simple_call_target(raw) {
        return None;
    }
    let mut parts: Vec<&str> = raw.split('.').collect();
    if parts.is_empty() {
        return None;
    }
    if parts[0] == "this" || parts[0] == "super" {
        parts.remove(0);
        if parts.is_empty() {
            return None;
        }
        let container = container_qualname(&ctx.module, &ctx.class_stack);
        return Some(format!("{container}.{}", parts.join(".")));
    }
    if parts.len() == 1 {
        let container = container_qualname(&ctx.module, &ctx.class_stack);
        return Some(format!("{container}.{raw}"));
    }
    Some(raw.to_string())
}

fn is_simple_call_target(raw: &str) -> bool {
    raw.chars()
        .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.' || ch == '$' || ch == '#')
}

/// A plain (non-arrow) JS/TS function expression or generator expression —
/// `function() {...}` / `function*() {...}`, named or anonymous, most often
/// seen as a callback. Unlike an arrow function, one of these dynamically
/// rebinds `this` (and `arguments`) to whatever the caller supplies at call
/// time, instead of inheriting the enclosing lexical `this`. Walking its
/// body with the *enclosing* scope's unchanged `Context` — same
/// `current_scope`, same `this`-relative resolution in `infer_receiver_type`
/// — would misattribute a `this.method()` call inside it to the wrong
/// class method, which is worse than not indexing the call at all. So
/// `walk_node` and `is_local_scope_boundary` both still treat this as a
/// hard boundary; see `is_lambda_node` below for the one kind that's safe
/// to fall through instead. (`"function"` is the bare `function` keyword
/// token itself — unnamed, so `named_children()` never yields it and this
/// arm is unreachable in practice — kept only for parity with the
/// pre-existing list this replaces.)
fn is_dynamic_this_function_node(kind: &str) -> bool {
    matches!(
        kind,
        "function" | "function_expression" | "generator_function"
    )
}

/// A JS/TS arrow function (`x => ...`, `(x, y) => ...`, `async (x) => ...`).
/// Always lexically captures the enclosing `this`/`arguments` — never
/// rebinds them like a plain `function` expression does (see
/// `is_dynamic_this_function_node`) — so it's safe for `walk_node` and
/// `collect_statement_bindings` to recurse straight through one with the
/// *same* `Context`/bindings map: it's a nested scope, not a new symbol.
/// Calls inside it (e.g. `.map(x => this.transform(x))`, `useEffect(() =>
/// fetchData(), [])`) attribute to the enclosing named symbol via
/// `ctx.current_scope`, and the arrow's own parameters are folded into the
/// enclosing `local_types` map — see `collect_statement_bindings`'s call
/// site — so a reference to one of them isn't mistaken for an outer name.
fn is_lambda_node(kind: &str) -> bool {
    kind == "arrow_function"
}

/// When `node` is the first function-like node inside a module-level
/// declarator's initializer (`ctx.fn_owner` set — see
/// `Context::fn_owner`), the context its body should be walked with: that
/// declarator's symbol as `current_scope`, mirroring what
/// `handle_function` does for a `function` declaration. An arrow keeps the
/// module's `local_types` (module-level inference already folds arrow
/// params/locals in — see `is_lambda_node`); a `function`/`function*`
/// expression or object-literal method gets its own, like
/// `handle_function`. Walking a plain `function` expression here is safe
/// despite `is_dynamic_this_function_node`: at module scope there is no
/// enclosing class for a `this.x()` to be misresolved against
/// (`class_attr_types` is empty). `fn_depth` is deliberately not bumped,
/// so a named `function` declared inside still gets its own symbol and
/// its calls, exactly as before.
// ponytail: object-literal properties get no symbols of their own, so
// `apiClient = { get: () => f() }` attributes `f` to `apiClient`, not
// `apiClient.get`.
fn owned_function_scope(node: Node<'_>, ctx: &Context, source: &str) -> Option<Context> {
    let owner = ctx.fn_owner.as_ref()?;
    let kind = node.kind();
    if !(is_lambda_node(kind) || is_dynamic_this_function_node(kind) || kind == "method_definition")
    {
        return None;
    }
    let mut next_ctx = ctx.clone();
    next_ctx.current_scope = owner.clone();
    next_ctx.fn_owner = None;
    if !is_lambda_node(kind) {
        next_ctx.local_types = Rc::new(infer_local_types(node, source));
    }
    Some(next_ctx)
}

/// Parameter names (+ inferred types, where explicitly annotated) bound by
/// an arrow function's `parameter` (single bare identifier, `x => ...`) or
/// `parameters` (parenthesized list, `(x, y) => ...`) field. Folded into
/// the *enclosing* function's `local_types` map by
/// `collect_statement_bindings` rather than given a scope of their own —
/// see `is_lambda_node`'s doc comment.
fn collect_lambda_parameter_bindings(
    node: Node<'_>,
    source: &str,
    bindings: &mut Vec<(String, LocalType)>,
) {
    if let Some(params) = node.child_by_field_name("parameters") {
        let mut cursor = params.walk();
        for param in params.named_children(&mut cursor) {
            collect_param_bindings(param, source, bindings);
        }
    } else if let Some(param) = node.child_by_field_name("parameter") {
        collect_param_bindings(param, source, bindings);
    }
}

fn handle_function(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    handle_function_named(node, ctx, source, output, node_text(name_node, source));
}

fn handle_function_named(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    name: String,
) {
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = extract_signature(node, source);
    // Export-based privacy is applied after the walk (`mark_unexported_private`).
    output.symbols.push(SymbolInput {
        kind: "function".to_string(),
        name: name.clone(),
        qualname: qualname.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature,
        docstring: None,
        identity: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(container_qualname(&ctx.module, &ctx.class_stack)),
        target_qualname: Some(qualname),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
    if let Some(body) = node.child_by_field_name("body") {
        let mut next_ctx = ctx.clone();
        next_ctx.fn_depth += 1;
        next_ctx.current_scope = build_qualname(&ctx.module, &ctx.class_stack, &name);
        next_ctx.local_types = Rc::new(infer_local_types(node, source));
        walk_node(body, &next_ctx, source, output);
    }
}

/// Whether `node` (a `method_definition`) is private: an explicit
/// `private` accessibility modifier (TypeScript), or a `#`-prefixed name
/// (a JS/TS private class field/method).
fn is_private_member(node: Node<'_>, source: &str) -> bool {
    let mut cursor = node.walk();
    let has_modifier = node
        .named_children(&mut cursor)
        .any(|c| c.kind() == "accessibility_modifier" && node_text(c, source) == "private");
    has_modifier
        || node
            .child_by_field_name("name")
            .is_some_and(|n| node_text(n, source).starts_with('#'))
}

fn handle_method(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = extract_signature(node, source);
    if is_private_member(node, source) {
        output.private_qualnames.push(qualname.clone());
    }
    output.symbols.push(SymbolInput {
        kind: "method".to_string(),
        name: name.clone(),
        qualname: qualname.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature,
        docstring: None,
        identity: None,
    });
    let parent = container_qualname(&ctx.module, &ctx.class_stack);
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(parent),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
    for edge in method_route_edges(node, &qualname, ctx, source) {
        output.edges.push(edge);
    }
    if name == "constructor" {
        handle_parameter_properties(node, ctx, source, output);
    }
    if let Some(body) = node.child_by_field_name("body") {
        let mut next_ctx = ctx.clone();
        next_ctx.fn_depth += 1;
        next_ctx.current_scope = build_qualname(&ctx.module, &ctx.class_stack, &name);
        next_ctx.local_types = Rc::new(infer_local_types(node, source));
        walk_node(body, &next_ctx, source, output);
    }
}

/// `namespace A.B { .. }` / `module M { .. }` / `declare module "x" { .. }`:
/// a `namespace` symbol per name segment (merged declarations share one),
/// with the body's members qualified through it.
fn handle_namespace(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if ctx.fn_depth > 0 {
        return;
    }
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let raw = node_text(name_node, source);
    let segments: Vec<String> = if name_node.kind() == "string" {
        vec![unquote_string_literal(&raw).unwrap_or(raw)]
    } else {
        raw.split('.').map(|s| s.trim().to_string()).collect()
    };
    let mut next_ctx = ctx.clone();
    for segment in segments.into_iter().filter(|s| !s.is_empty()) {
        let qualname = build_qualname(&ctx.module, &next_ctx.class_stack, &segment);
        if !output.symbols.iter().any(|s| s.qualname == qualname) {
            let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
            output.symbols.push(SymbolInput {
                kind: "namespace".to_string(),
                name: segment.clone(),
                qualname: qualname.clone(),
                start_line,
                start_col,
                end_line,
                end_col,
                start_byte,
                end_byte,
                signature: None,
                docstring: None,
                identity: None,
            });
            output.edges.push(EdgeInput {
                kind: "CONTAINS".to_string(),
                source_qualname: Some(container_qualname(&ctx.module, &next_ctx.class_stack)),
                target_qualname: Some(qualname.clone()),
                detail: None,
                evidence_snippet: None,
                ..Default::default()
            });
        }
        next_ctx.class_stack.push(segment);
        next_ctx.ns_depth += 1;
        next_ctx.current_scope = qualname;
    }
    if let Some(body) = node.child_by_field_name("body") {
        let mut cursor = body.walk();
        for child in body.named_children(&mut cursor) {
            walk_node(child, &next_ctx, source, output);
        }
    }
}

/// `export default <anonymous function/class/expression>`: indexes it as the
/// symbol `DEFAULT_EXPORT` so an importer of the default can resolve to it.
/// Returns `true` when the statement was fully handled.
fn handle_anonymous_default(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) -> bool {
    if ctx.fn_depth > 0 || !ctx.class_stack.is_empty() {
        return false;
    }
    let mut cursor = node.walk();
    let is_default = node.children(&mut cursor).any(|c| c.kind() == "default");
    let Some(value) = node.child_by_field_name("value") else {
        return false;
    };
    // `export default Foo;` re-exports a named local.
    if !is_default || value.kind() == "identifier" {
        return false;
    }
    let qualname = build_qualname(&ctx.module, &ctx.class_stack, DEFAULT_EXPORT);
    match value.kind() {
        "function_expression" | "function" | "generator_function" | "arrow_function" => {
            handle_function_named(value, ctx, source, output, DEFAULT_EXPORT.to_string());
        }
        "class" => {
            handle_class_named(value, ctx, source, output, DEFAULT_EXPORT.to_string());
        }
        _ => {
            let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
            output.symbols.push(SymbolInput {
                kind: "const".to_string(),
                name: DEFAULT_EXPORT.to_string(),
                qualname: qualname.clone(),
                start_line,
                start_col,
                end_line,
                end_col,
                start_byte,
                end_byte,
                signature: None,
                docstring: None,
                identity: None,
            });
            output.edges.push(EdgeInput {
                kind: "CONTAINS".to_string(),
                source_qualname: Some(ctx.module.clone()),
                target_qualname: Some(qualname.clone()),
                detail: None,
                evidence_snippet: None,
                ..Default::default()
            });
            let mut next_ctx = ctx.clone();
            next_ctx.fn_owner = Some(qualname);
            walk_node(value, &next_ctx, source, output);
        }
    }
    true
}

fn handle_named_item(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    kind: &str,
) -> Option<String> {
    let name_node = node.child_by_field_name("name")?;
    let name = node_text(name_node, source);
    if name.is_empty() {
        return None;
    }
    let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    output.symbols.push(SymbolInput {
        kind: kind.to_string(),
        name: name.clone(),
        qualname: qualname.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature: None,
        docstring: None,
        identity: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(container_qualname(&ctx.module, &ctx.class_stack)),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
    Some(qualname)
}

fn handle_variable_declaration(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    if ctx.class_stack.len() > ctx.ns_depth || is_local_declaration(node) {
        return;
    }
    let decl_kind = declaration_keyword(node, source);
    let kind = if decl_kind == "const" {
        "const"
    } else {
        "variable"
    };
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "variable_declarator" {
            continue;
        }
        let Some(name_node) = child.child_by_field_name("name") else {
            continue;
        };
        // A destructuring pattern yields one symbol per bound identifier,
        // never one named by the pattern text.
        // `const { x } = require(..)` / `await import(..)` are imports, not
        // declarations.
        if name_node.kind() != "identifier" && is_require_or_import_init(child, source) {
            continue;
        }
        let mut names = Vec::new();
        collect_binding_names(name_node, source, &mut names);
        for name in names {
            let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
            let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(child);
            output.symbols.push(SymbolInput {
                kind: kind.to_string(),
                name,
                qualname: qualname.clone(),
                start_line,
                start_col,
                end_line,
                end_col,
                start_byte,
                end_byte,
                signature: None,
                docstring: None,
                identity: None,
            });
            output.edges.push(EdgeInput {
                kind: "CONTAINS".to_string(),
                source_qualname: Some(container_qualname(&ctx.module, &ctx.class_stack)),
                target_qualname: Some(qualname),
                detail: None,
                evidence_snippet: None,
                ..Default::default()
            });
        }
    }
}

/// True when a declarator's initializer is `require(..)`, `import(..)`, or
/// an `await` of either.
fn is_require_or_import_init(declarator: Node<'_>, source: &str) -> bool {
    let Some(mut value) = declarator.child_by_field_name("value") else {
        return false;
    };
    while value.kind() == "await_expression" || value.kind() == "parenthesized_expression" {
        let Some(inner) = value.named_child(0) else {
            return false;
        };
        value = inner;
    }
    value.kind() == "call_expression"
        && value.child_by_field_name("function").is_some_and(|f| {
            f.kind() == "import" || (f.kind() == "identifier" && node_text(f, source) == "require")
        })
}

/// Collects the identifiers a binding pattern introduces: plain identifiers,
/// shorthand (`{ a }`), renames (`{ b: c }` -> `c`), defaults, rest and
/// nested object/array patterns.
fn collect_binding_names(node: Node<'_>, source: &str, out: &mut Vec<String>) {
    match node.kind() {
        "identifier" | "shorthand_property_identifier_pattern" => {
            let name = node_text(node, source);
            if !name.is_empty() {
                out.push(name);
            }
        }
        // `{ k: pattern }` binds only the value side.
        "pair_pattern" => {
            if let Some(value) = node.child_by_field_name("value") {
                collect_binding_names(value, source, out);
            }
        }
        // `{ a = 1 }` / `[a = 1]` bind only the left side.
        "object_assignment_pattern" | "assignment_pattern" => {
            if let Some(left) = node.child_by_field_name("left") {
                collect_binding_names(left, source, out);
            }
        }
        "object_pattern" | "array_pattern" | "rest_pattern" => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                collect_binding_names(child, source, out);
            }
        }
        _ => {}
    }
}

/// A declaration is module scope only if every ancestor up to `program` is
/// the program itself, an `export`/`declare` wrapper, or a TS namespace (its
/// wrapper, node and body block). Anything else (function/arrow/method
/// bodies, `if`/`for`/`switch` blocks, ...) is local.
fn is_local_declaration(node: Node<'_>) -> bool {
    let mut cur = node.parent();
    while let Some(n) = cur {
        let ok = match n.kind() {
            "program"
            | "export_statement"
            | "ambient_declaration"
            | "expression_statement"
            | "internal_module"
            | "module" => true,
            "statement_block" => matches!(
                n.parent().map(|p| p.kind()),
                Some("internal_module" | "module")
            ),
            _ => false,
        };
        if !ok {
            return true;
        }
        cur = n.parent();
    }
    false
}

fn declaration_keyword(node: Node<'_>, source: &str) -> &'static str {
    let text = node_text(node, source);
    let trimmed = text.trim_start();
    if trimmed.starts_with("const ") {
        "const"
    } else if trimmed.starts_with("let ") {
        "let"
    } else {
        "var"
    }
}

fn handle_import(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    allow_fallback: bool,
) {
    let target = match extract_import_target(node, source, allow_fallback) {
        Some(value) => value,
        None => return,
    };
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    output.edges.push(EdgeInput {
        kind: "IMPORTS".to_string(),
        source_qualname: Some(ctx.module.clone()),
        target_qualname: Some(target),
        detail: None,
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    });
}

fn extract_import_target(node: Node<'_>, source: &str, allow_fallback: bool) -> Option<String> {
    if let Some(source_node) = node.child_by_field_name("source") {
        let raw = node_text(source_node, source);
        return unquote_string_literal(&raw).or(Some(raw));
    }
    if !allow_fallback {
        return None;
    }
    let raw = node_text(node, source);
    extract_string_from_text(&raw)
}

fn extract_string_from_text(raw: &str) -> Option<String> {
    let chars = raw.char_indices();
    let mut quote = None;
    let mut start = 0;
    for (idx, ch) in chars {
        if ch == '"' || ch == '\'' || ch == '`' {
            quote = Some(ch);
            start = idx + ch.len_utf8();
            break;
        }
    }
    let quote = quote?;
    let rest = &raw[start..];
    let end = rest.find(quote)?;
    Some(rest[..end].to_string())
}

fn unquote_string_literal(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.len() < 2 {
        return None;
    }
    let first = trimmed.chars().next()?;
    if first == '"' || first == '\'' || first == '`' {
        let last = trimmed.chars().last()?;
        if last == first {
            return Some(trimmed[1..trimmed.len() - 1].to_string());
        }
    }
    None
}

fn extract_signature(node: Node<'_>, source: &str) -> Option<String> {
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(n, source));
    params.filter(|value| !value.is_empty())
}

fn build_qualname(module: &str, class_stack: &[String], name: &str) -> String {
    if class_stack.is_empty() {
        format!("{module}.{name}")
    } else {
        format!("{module}.{}.{}", class_stack.join("."), name)
    }
}

fn container_qualname(module: &str, class_stack: &[String]) -> String {
    if class_stack.is_empty() {
        module.to_string()
    } else {
        format!("{module}.{}", class_stack.join("."))
    }
}

const JS_TS_BUILTIN_TYPES: &[&str] = &[
    "string",
    "number",
    "boolean",
    "any",
    "unknown",
    "never",
    "void",
    "object",
    "symbol",
    "bigint",
    "null",
    "undefined",
    "this",
    "Array",
    "Object",
    "String",
    "Number",
    "Boolean",
    "Function",
    "Date",
    "RegExp",
    "Promise",
    "Map",
    "Set",
    "WeakMap",
    "WeakSet",
    "Error",
    "TypeError",
    "RangeError",
    "Symbol",
    "BigInt",
    "Record",
    "Partial",
    "Required",
    "Readonly",
    "ReadonlyArray",
    "Pick",
    "Omit",
    "Exclude",
    "Extract",
    "NonNullable",
    "JSON",
    "Math",
    "Buffer",
];

/// Global JS/TS *runtime* constructors/functions a bare `Name(...)`/`new
/// Name(...)` call can never mean a repo symbol for (issue #110: `new
/// Error(...)` must not fuzzy-bind to an unrelated same-named `error`
/// elsewhere). Deliberately a separate, smaller list than
/// `JS_TS_BUILTIN_TYPES`: that one also carries TypeScript type-only names
/// (`Record`, `Pick`, `Omit`, ...) with no runtime existence at all, which
/// a repo could plausibly also declare as its own same-named runtime
/// function/class (`export function pick(...)` or, now that resolution is
/// case-sensitive, even `class Pick`) — gating call resolution on those
/// would risk a false "never binds" for a real repo symbol. Every name
/// here is instead a real global `new`/call target with no legitimate
/// same-named repo meaning.
const JS_TS_GLOBAL_CALLABLES: &[&str] = &[
    "Error",
    "TypeError",
    "RangeError",
    "SyntaxError",
    "ReferenceError",
    "EvalError",
    "URIError",
    "Array",
    "Object",
    "String",
    "Number",
    "Boolean",
    "Function",
    "Date",
    "RegExp",
    "Promise",
    "Map",
    "Set",
    "WeakMap",
    "WeakSet",
    "Symbol",
    "BigInt",
    "Proxy",
    "URL",
    "URLSearchParams",
    "Request",
    "Response",
    "Headers",
    "FormData",
    "Blob",
    "AbortController",
    "TextEncoder",
    "TextDecoder",
    "WeakRef",
];

/// Whether `name` is a `JS_TS_GLOBAL_CALLABLES` entry not shadowed in
/// `ctx` — by a top-level `import` binding of that name, or by a
/// function-local variable/parameter (`ctx.local_types`). A same-named
/// symbol declared elsewhere at module scope in *this* file is not
/// checked here: `Resolver::resolve`'s exact-qualname tier already runs
/// before `receiver_type` is even consulted, so a genuine local
/// `class Error {}` still resolves through that tier regardless of what
/// this function returns — this only ever gates the *fuzzy* fallback
/// tiers (see `infer_receiver_type`'s doc).
fn is_unshadowed_global_callable(name: &str, ctx: &Context) -> bool {
    JS_TS_GLOBAL_CALLABLES.contains(&name)
        && !ctx.import_bindings.contains_key(name)
        && !ctx.local_types.contains_key(name)
}

/// Infer the receiver type of a call's callee expression (`function_node`),
/// mirroring `python::infer_receiver_type` with `this` standing in for
/// `self`/`cls`. Only gates resolution; never changes `target_qualname`
/// (see `resolve_call_target`, which stays text-based and keeps the
/// receiver's literal text for evidence).
///
/// Rules, in order:
/// - A bare identifier naming a JS/TS runtime global (`new Error(...)`,
///   `Symbol(...)`, ...) not shadowed by an import or a local — see
///   `JS_TS_GLOBAL_CALLABLES` — → `Unresolved`: never a repo symbol,
///   whatever else in the index happens to share its name (issue #110).
/// - Not a member access at all (`helper()`) → `NotTracked` (bare call,
///   nothing to gate).
/// - `super.method()` (any depth) → `NotTracked`: `resolve_call_target`
///   already maps `super.x` onto the enclosing class exactly like `this.x`;
///   left exactly as-is, not asked for by this task.
/// - `this.method()` (zero hops) → `NotTracked`, already resolved exactly
///   via `resolve_call_target`'s container-qualname path.
/// - `this.field.method()` (exactly one hop off `this`) → resolved via a
///   type-annotated class field or constructor parameter property, if any;
///   otherwise `Unresolved`.
/// - `X.method()` where `X` is a bare identifier: `Known`/`Unresolved` from
///   this function's local types if `X` is tracked, else `NotTracked` (a
///   class/module/namespace reference, e.g. `Console.log()`).
/// - Anything deeper, or a chain rooted in something other than a bare
///   identifier/`this` (a call result, a parenthesized/cast expression,
///   ...), → `Unresolved` if the root is `this` or a tracked local,
///   `NotTracked` otherwise.
fn infer_receiver_type(function_node: Node<'_>, source: &str, ctx: &Context) -> ReceiverType {
    if function_node.kind() == "identifier" {
        let name = node_text(function_node, source);
        if is_unshadowed_global_callable(&name, ctx) {
            return ReceiverType::Unresolved;
        }
    }
    if function_node.kind() != "member_expression"
        && function_node.kind() != "optional_member_expression"
    {
        return ReceiverType::NotTracked;
    }
    let Some(object) = function_node.child_by_field_name("object") else {
        return ReceiverType::NotTracked;
    };
    let (root, hops) = member_chain_root(object);

    if root.kind() == "super" {
        return ReceiverType::NotTracked;
    }

    if root.kind() == "this" {
        if hops == 0 {
            return ReceiverType::NotTracked;
        }
        if hops == 1 {
            let attr_name = object
                .child_by_field_name("property")
                .map(|n| node_text(n, source));
            return match attr_name.and_then(|name| ctx.class_attr_types.get(&name).cloned()) {
                Some(LocalType::Known(ty)) => ReceiverType::Known(ty),
                _ => ReceiverType::Unresolved,
            };
        }
        // ponytail: deeper chains (`this.a.b.method()`) would need real
        // attribute-type inference across assignments — out of scope, same
        // ceiling as `python::infer_receiver_type`.
        return ReceiverType::Unresolved;
    }

    if root.kind() != "identifier" {
        // Chain rooted in a call result, parenthesized/cast expression,
        // subscript, etc. — not inferable.
        return ReceiverType::Unresolved;
    }
    let root_name = node_text(root, source);
    if hops == 0 {
        return match ctx.local_types.get(&root_name) {
            Some(LocalType::Known(ty)) => ReceiverType::Known(ty.clone()),
            Some(LocalType::Other) => ReceiverType::Unresolved,
            None => ReceiverType::NotTracked,
        };
    }
    if ctx.local_types.contains_key(&root_name) {
        ReceiverType::Unresolved
    } else {
        ReceiverType::NotTracked
    }
}

/// Walk a (possibly nested) member-access chain down to its root node,
/// returning the root plus how many hops separate it from `node` (0 =
/// `node` itself is the root).
fn member_chain_root(node: Node<'_>) -> (Node<'_>, usize) {
    let mut current = node;
    let mut hops = 0;
    while current.kind() == "member_expression" || current.kind() == "optional_member_expression" {
        match current.child_by_field_name("object") {
            Some(obj) => {
                current = obj;
                hops += 1;
            }
            None => break,
        }
    }
    (current, hops)
}

/// Classify a type-annotation (or bare constructor-name) expression's text
/// into a `LocalType`. Generic/union/array/object-literal type shapes are
/// never unwrapped — they collapse to `Other` just like a builtin would,
/// mirroring `python::classify_annotation`'s identical ponytail simplification.
fn classify_annotation(text: &str) -> LocalType {
    let text = text.trim();
    if text.is_empty() {
        return LocalType::Other;
    }
    if text.contains(['<', '[', '|', '&', '(', ')', '{']) {
        return LocalType::Other;
    }
    let bare = text.rsplit('.').next().unwrap_or(text).trim();
    classify_type_name(bare)
}

fn classify_type_name(name: &str) -> LocalType {
    if name.is_empty() || JS_TS_BUILTIN_TYPES.contains(&name) {
        LocalType::Other
    } else {
        LocalType::Known(name.to_string())
    }
}

/// Extract a type annotation's text from its wrapping `type_annotation`
/// node (the `: Type` suffix), unwrapped to just `Type`.
fn annotation_text(type_annotation: Node<'_>, source: &str) -> String {
    match type_annotation.named_child(0) {
        Some(inner) => node_text(inner, source),
        None => node_text(type_annotation, source),
    }
}

/// Classify a variable/field initializer's shape into a `LocalType` when no
/// explicit type annotation is present. The only `Known` case is direct
/// construction (`new EventStore()`); everything else (array/object/string
/// literals, another call's return value, ...) is `Other` — mirrors
/// `python::classify_assignment_value`'s identical ceiling.
fn classify_value_expr(value: Node<'_>, source: &str) -> LocalType {
    if value.kind() == "new_expression"
        && let Some(ctor) = value.child_by_field_name("constructor")
    {
        return classify_annotation(&node_text(ctor, source));
    }
    LocalType::Other
}

/// Infer types for names bound within a single function body: parameters
/// and `const`/`let`/`var` declarations. Scope is strictly this function —
/// never a caller, a callee, or another method of the same class (see
/// `Context::local_types`'s doc comment). No CALLS edges are ever extracted
/// from inside a nested plain `function`/`function*` expression (see
/// `is_dynamic_this_function_node`, which stops `walk_node` there
/// entirely), so this deliberately doesn't recurse into one either. An
/// arrow function is different — see `is_lambda_node` — so
/// `collect_statement_bindings` (which this calls into) does recurse into
/// one of those, folding its parameters into this same map.
fn infer_local_types(function_node: Node<'_>, source: &str) -> HashMap<String, LocalType> {
    let mut bindings: Vec<(String, LocalType)> = Vec::new();
    if let Some(params) = function_node.child_by_field_name("parameters") {
        let mut cursor = params.walk();
        for param in params.named_children(&mut cursor) {
            collect_param_bindings(param, source, &mut bindings);
        }
    }
    if let Some(body) = function_node.child_by_field_name("body") {
        collect_statement_bindings(body, source, &mut bindings);
    }
    bindings_to_local_types(bindings)
}

/// Infer types for names bound directly at module top level — its own
/// single scope, exactly like a function body is; see
/// `python::infer_module_level_types`.
fn infer_module_level_types(root: Node<'_>, source: &str) -> HashMap<String, LocalType> {
    let mut bindings: Vec<(String, LocalType)> = Vec::new();
    collect_statement_bindings(root, source, &mut bindings);
    bindings_to_local_types(bindings)
}

/// Fold a scope's raw (name, inferred-type) bindings into a lookup map,
/// with a name bound more than once anywhere in the scope collapsing to
/// `Other` — mirrors `python::bindings_to_local_types`.
fn bindings_to_local_types(bindings: Vec<(String, LocalType)>) -> HashMap<String, LocalType> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for (name, _) in &bindings {
        *counts.entry(name.clone()).or_default() += 1;
    }
    let mut result = HashMap::new();
    for (name, ty) in bindings {
        let reassigned = counts.get(&name).copied().unwrap_or(0) > 1;
        result.insert(name, if reassigned { LocalType::Other } else { ty });
    }
    result
}

fn collect_param_bindings(param: Node<'_>, source: &str, bindings: &mut Vec<(String, LocalType)>) {
    match param.kind() {
        "identifier" => bindings.push((node_text(param, source), LocalType::Other)),
        "required_parameter" | "optional_parameter" => {
            let Some(pattern) = param.child_by_field_name("pattern") else {
                return;
            };
            if pattern.kind() == "identifier" {
                let name = node_text(pattern, source);
                let ty = param
                    .child_by_field_name("type")
                    .map(|t| classify_annotation(&annotation_text(t, source)))
                    .unwrap_or(LocalType::Other);
                bindings.push((name, ty));
            } else {
                collect_pattern_identifiers(pattern, source, bindings);
            }
        }
        "assignment_pattern" => {
            if let Some(left) = param.child_by_field_name("left") {
                collect_pattern_identifiers(left, source, bindings);
            }
        }
        "rest_pattern" => {
            if let Some(inner) = param.named_child(0) {
                collect_pattern_identifiers(inner, source, bindings);
            }
        }
        "object_pattern" | "array_pattern" => {
            collect_pattern_identifiers(param, source, bindings);
        }
        _ => {}
    }
}

/// Collect every identifier bound by a (possibly nested) destructuring
/// pattern — array/object patterns, renamed/default/rest sub-patterns —
/// each pushed as `LocalType::Other`. Used for both destructured parameters
/// and destructured `const`/`let`/`var`/`for`/`catch` targets; see
/// `python::collect_pattern_identifiers` for why these must be tracked at
/// all (as opposed to left absent from the map).
fn collect_pattern_identifiers(
    node: Node<'_>,
    source: &str,
    bindings: &mut Vec<(String, LocalType)>,
) {
    match node.kind() {
        "identifier" | "shorthand_property_identifier_pattern" => {
            bindings.push((node_text(node, source), LocalType::Other));
        }
        "array_pattern" | "object_pattern" => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                collect_pattern_identifiers(child, source, bindings);
            }
        }
        "pair_pattern" => {
            if let Some(value) = node.child_by_field_name("value") {
                collect_pattern_identifiers(value, source, bindings);
            }
        }
        "assignment_pattern" | "object_assignment_pattern" => {
            if let Some(left) = node.child_by_field_name("left") {
                collect_pattern_identifiers(left, source, bindings);
            }
        }
        "rest_pattern" => {
            if let Some(inner) = node.named_child(0) {
                collect_pattern_identifiers(inner, source, bindings);
            }
        }
        _ => {}
    }
}

/// Node kinds that introduce a fresh local-type scope of their own — a
/// separate function/method/class body, never inherited from the outer
/// scope being walked. Mirrors `is_dynamic_this_function_node` plus the
/// declaration/class-body kinds that function doesn't need to cover (its
/// callers already stop at those separately). `arrow_function` is
/// deliberately *not* included — see `is_lambda_node`'s doc comment and
/// this function's call site below.
fn is_local_scope_boundary(kind: &str) -> bool {
    matches!(
        kind,
        "function_declaration"
            | "generator_function_declaration"
            | "function_expression"
            | "generator_function"
            | "method_definition"
            | "class_declaration"
            | "abstract_class_declaration"
            | "class"
    )
}

/// Recursively collect local-variable bindings from statements within a
/// single function body (or module top level), stopping at nested
/// function/class boundaries (their own locals are a different scope
/// entirely — see `Context::local_types`'s doc comment). An arrow function
/// is *not* a boundary here — see `is_lambda_node`'s doc comment — so a
/// call inside one is walked with the *enclosing* function's
/// `local_types`, and the arrow's own parameters are folded into that same
/// map below (mirrors `python::collect_statement_bindings`'s `"lambda"`
/// arm) so a reference to one of them isn't mistaken for an outer name.
fn collect_statement_bindings(
    node: Node<'_>,
    source: &str,
    bindings: &mut Vec<(String, LocalType)>,
) {
    if is_local_scope_boundary(node.kind()) {
        return;
    }
    if is_lambda_node(node.kind()) {
        collect_lambda_parameter_bindings(node, source, bindings);
        // No `return`: still recurse into children below (the body may
        // declare further locals, or contain a nested arrow function whose
        // own parameters also need folding in).
    }
    match node.kind() {
        "lexical_declaration" | "variable_declaration" => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if child.kind() == "variable_declarator" {
                    collect_variable_declarator_binding(child, source, bindings);
                }
            }
        }
        "for_in_statement" => {
            if let Some(left) = node.child_by_field_name("left") {
                collect_pattern_identifiers(left, source, bindings);
            }
        }
        "catch_clause" => {
            if let Some(param) = node.child_by_field_name("parameter") {
                collect_pattern_identifiers(param, source, bindings);
            }
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_statement_bindings(child, source, bindings);
    }
}

/// A `let y; y = new Foo();` delayed reassignment is deliberately not
/// tracked — only a `variable_declarator`'s own type/initializer counts, so
/// a name declared without either always collapses to `Other`.
fn collect_variable_declarator_binding(
    node: Node<'_>,
    source: &str,
    bindings: &mut Vec<(String, LocalType)>,
) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    if name_node.kind() != "identifier" {
        collect_pattern_identifiers(name_node, source, bindings);
        return;
    }
    let name = node_text(name_node, source);
    let ty = if let Some(type_node) = node.child_by_field_name("type") {
        classify_annotation(&annotation_text(type_node, source))
    } else if let Some(value_node) = node.child_by_field_name("value") {
        classify_value_expr(value_node, source)
    } else {
        LocalType::Other
    };
    bindings.push((name, ty));
}

/// Type-annotated fields (`public_field_definition` / `field_definition`)
/// and typed constructor parameter properties
/// (`constructor(private store: EventStore)`) declared directly in a class
/// body — not inside any other method. Used only to resolve a single-hop
/// `this.field.method()` receiver; see `infer_receiver_type`.
fn collect_class_level_attr_types(
    class_body: Node<'_>,
    source: &str,
) -> HashMap<String, LocalType> {
    let mut result = HashMap::new();
    let mut cursor = class_body.walk();
    for member in class_body.named_children(&mut cursor) {
        match member.kind() {
            "public_field_definition" | "field_definition" => {
                let Some(name_node) = member.child_by_field_name("name") else {
                    continue;
                };
                let Some(type_node) = member.child_by_field_name("type") else {
                    continue;
                };
                let name = node_text(name_node, source);
                if name.is_empty() {
                    continue;
                }
                result.insert(
                    name,
                    classify_annotation(&annotation_text(type_node, source)),
                );
            }
            "method_definition" => {
                let is_ctor = member
                    .child_by_field_name("name")
                    .map(|n| node_text(n, source) == "constructor")
                    .unwrap_or(false);
                if !is_ctor {
                    continue;
                }
                let Some(params) = member.child_by_field_name("parameters") else {
                    continue;
                };
                let mut pcursor = params.walk();
                for param in params.named_children(&mut pcursor) {
                    if !matches!(param.kind(), "required_parameter" | "optional_parameter") {
                        continue;
                    }
                    // ponytail: a parameter property is only recognized via
                    // an explicit accessibility modifier (public/private/
                    // protected); a bare `readonly` alone is structurally
                    // indistinguishable from a plain parameter in this
                    // tree-sitter-typescript version, so it isn't tracked.
                    let mut mcursor = param.walk();
                    let has_modifier = param
                        .named_children(&mut mcursor)
                        .any(|c| c.kind() == "accessibility_modifier");
                    if !has_modifier {
                        continue;
                    }
                    let Some(pattern) = param.child_by_field_name("pattern") else {
                        continue;
                    };
                    if pattern.kind() != "identifier" {
                        continue;
                    }
                    let name = node_text(pattern, source);
                    let ty = param
                        .child_by_field_name("type")
                        .map(|t| classify_annotation(&annotation_text(t, source)))
                        .unwrap_or(LocalType::Other);
                    result.insert(name, ty);
                }
            }
            _ => {}
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{
        JavascriptExtractor, grpc_service_from_path, match_alias_pattern, strip_jsonc,
        substitute_alias_target,
    };
    use crate::indexer::extract::{LanguageExtractor, ReceiverType};
    use crate::indexer::http;
    use crate::indexer::proto;

    #[test]
    fn extracts_express_route_and_fetch_call() {
        let source = r#"
const app = require("express")();
function handler(req, res) {}
app.get("/api/users/:id", handler);
fetch("/api/users/123", { method: "POST" });
"#;
        let mut extractor = JavascriptExtractor::new().unwrap();
        let file = extractor.extract(source, "index").unwrap();
        let routes = file
            .edges
            .iter()
            .filter(|edge| edge.kind == http::HTTP_ROUTE_KIND)
            .collect::<Vec<_>>();
        let calls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == http::HTTP_CALL_KIND)
            .collect::<Vec<_>>();
        assert!(
            routes
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/api/users/{}"))
        );
        assert!(
            calls
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/api/users/{}"))
        );
    }

    fn ts_edges(source: &str, module: &str, kind: &str) -> Vec<(String, String)> {
        let mut extractor = super::TypescriptExtractor::new().unwrap();
        let file = extractor.extract(source, module).unwrap();
        file.edges
            .iter()
            .filter(|e| e.kind == kind)
            .map(|e| {
                let detail: serde_json::Value =
                    serde_json::from_str(e.detail.as_deref().unwrap_or("{}")).unwrap();
                (
                    detail["method"].as_str().unwrap_or("").to_string(),
                    e.target_qualname.clone().unwrap_or_default(),
                )
            })
            .collect()
    }

    #[test]
    fn next_app_route_emits_one_route_per_exported_method() {
        let source = r#"
export async function GET(req: Request) { return Response.json([]); }
export const POST = async (req: Request) => Response.json({});
const h = () => new Response();
export { h as DELETE };
export function helper() {}
"#;
        let routes = ts_edges(
            source,
            "web/src/app/api/tables/route",
            http::HTTP_ROUTE_KIND,
        );
        assert_eq!(
            routes,
            vec![
                ("GET".to_string(), "/api/tables".to_string()),
                ("POST".to_string(), "/api/tables".to_string()),
                ("DELETE".to_string(), "/api/tables".to_string()),
            ]
        );
    }

    #[test]
    fn next_app_route_outside_api_and_with_groups_and_params() {
        let source = "export function GET() { return new Response(); }";
        let routes = ts_edges(
            source,
            "apps/web/app/(admin)/@modal/users/[id]/[...rest]/route",
            http::HTTP_ROUTE_KIND,
        );
        assert_eq!(
            routes,
            vec![("GET".to_string(), "/users/{}/{}".to_string())]
        );
        assert!(ts_edges(source, "app/_private/x/route", http::HTTP_ROUTE_KIND).is_empty());
    }

    #[test]
    fn next_app_route_without_method_exports_falls_back_to_any() {
        let routes = ts_edges(
            "export const dynamic = 'force-dynamic';",
            "app/health/route",
            http::HTTP_ROUTE_KIND,
        );
        assert_eq!(routes, vec![("ANY".to_string(), "/health".to_string())]);
    }

    #[test]
    fn next_pages_api_keeps_any_and_api_prefix() {
        let routes = ts_edges(
            "export default function handler() {}",
            "src/pages/api/users/[id]",
            http::HTTP_ROUTE_KIND,
        );
        assert_eq!(
            routes,
            vec![("ANY".to_string(), "/api/users/{}".to_string())]
        );
        assert!(
            ts_edges(
                "export default function P() {}",
                "utils/pages",
                http::PAGE_ROUTE_KIND
            )
            .is_empty()
        );
        assert_eq!(
            ts_edges(
                "export default function P() {}",
                "web/app/(x)/dash/page",
                http::PAGE_ROUTE_KIND
            )
            .len(),
            1
        );
    }

    #[test]
    fn template_literal_urls_produce_http_calls() {
        let source = r#"
async function load(n: number, id: string) {
  await fetch(`${BASE}/api/tables?limit=${n}`);
  await fetch(`${process.env.API_URL}/api/tables/${id}/columns#x`, { method: "POST" });
  await fetch(`/api/static`);
  await fetch(`${BASE}/api/things${suffix}`);
  await fetch(`${BASE}`);
  await fetch(`${a}${b}/api/x`);
  await axios.get(`https://example.com/api/v1/items/${id}?q=1`);
}
"#;
        let calls = ts_edges(source, "client", http::HTTP_CALL_KIND);
        assert_eq!(
            calls,
            vec![
                ("GET".to_string(), "/api/tables".to_string()),
                ("POST".to_string(), "/api/tables/{}/columns".to_string()),
                ("GET".to_string(), "/api/static".to_string()),
                ("GET".to_string(), "/api/things".to_string()),
                ("GET".to_string(), "/api/v1/items/{}".to_string()),
            ]
        );
    }

    #[test]
    fn axios_config_object_accepts_template_url() {
        let source = r#"
async function go() {
  await axios({ url: `${BASE}/api/tables`, method: "POST" });
}
"#;
        assert_eq!(
            ts_edges(source, "client", http::HTTP_CALL_KIND),
            vec![("POST".to_string(), "/api/tables".to_string())]
        );
    }

    #[test]
    fn next_root_must_be_conventional() {
        for module in ["src/features/pages/list", "src/components/app/route"] {
            let src = "export function GET() {}";
            assert!(
                ts_edges(src, module, http::PAGE_ROUTE_KIND).is_empty(),
                "{module}"
            );
            assert!(
                ts_edges(src, module, http::HTTP_ROUTE_KIND).is_empty(),
                "{module}"
            );
        }
        for (module, kind, path) in [
            ("web/pages/list", http::PAGE_ROUTE_KIND, "/list"),
            ("apps/web/pages/list", http::PAGE_ROUTE_KIND, "/list"),
            (
                "apps/web/src/app/api/x/route",
                http::HTTP_ROUTE_KIND,
                "/api/x",
            ),
        ] {
            let got = ts_edges("export function GET() {}", module, kind);
            assert_eq!(got.len(), 1, "{module}");
            assert_eq!(got[0].1, path);
        }
    }

    #[test]
    fn axios_instances_produce_http_calls_with_base_url() {
        let source = r#"
const api = axios.create({ baseURL: "/api" });
export const other = axios.create({ baseURL: `${HOST}/v2` });
let bare = axios.create();
async function go(id: string) {
  await api.get("/tables");
  await api.post(`/tables/${id}`, {});
  await api.request({ url: "/tables", method: "DELETE" });
  await other.get("/things");
  await bare.get(`/api/plain`);
  await api({ url: "/direct" });
  await unknown.get("/nope");
}
"#;
        assert_eq!(
            ts_edges(source, "client", http::HTTP_CALL_KIND),
            vec![
                ("GET".to_string(), "/api/tables".to_string()),
                ("POST".to_string(), "/api/tables/{}".to_string()),
                ("DELETE".to_string(), "/api/tables".to_string()),
                ("GET".to_string(), "/v2/things".to_string()),
                ("GET".to_string(), "/api/plain".to_string()),
                ("GET".to_string(), "/api/direct".to_string()),
            ]
        );
        // `api` is also an Express router receiver name; an axios instance must not
        // be read as a route definition.
        assert!(ts_edges(source, "client", http::HTTP_ROUTE_KIND).is_empty());
    }

    #[test]
    fn extracts_grpc_js_impl_and_call() {
        let source = r#"
const grpc = require("@grpc/grpc-js");
const proto = { helloworld: { Greeter: { service: {} } } };
function sayHello(call, callback) {}
const server = new grpc.Server();
server.addService(proto.helloworld.Greeter.service, { sayHello });
const client = new proto.helloworld.Greeter("localhost:50051", grpc.credentials.createInsecure());
client.sayHello({ name: "world" }, () => {});
"#;
        let mut extractor = JavascriptExtractor::new().unwrap();
        let file = extractor.extract(source, "index").unwrap();
        let impls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_IMPL_KIND)
            .collect::<Vec<_>>();
        let calls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_CALL_KIND)
            .collect::<Vec<_>>();
        assert!(impls.iter().any(|edge| {
            edge.target_qualname.as_deref() == Some("/helloworld.greeter/sayhello")
        }));
        assert!(calls.iter().any(|edge| {
            edge.target_qualname.as_deref() == Some("/helloworld.greeter/sayhello")
        }));
    }

    #[test]
    fn grpc_service_from_path_collapses_interior_whitespace() {
        // Direct unit-level proof for the `144d675`-style fix: a gRPC
        // client's constructor path split across lines (formatting, same
        // semantics) must resolve identically to the single-line form.
        let single_line =
            grpc_service_from_path("proto.helloworld.Greeter").expect("single-line path");
        let multi_line = grpc_service_from_path("proto.helloworld\n    .Greeter")
            .expect("multi-line path must resolve just like the single-line form");
        assert_eq!(
            multi_line.package.as_deref(),
            single_line.package.as_deref()
        );
        assert_eq!(multi_line.service, single_line.service);
        assert_eq!(multi_line.package.as_deref(), Some("helloworld"));
        assert_eq!(multi_line.service, "Greeter");
    }

    #[test]
    fn grpc_client_multiline_receiver_resolves_like_single_line() {
        // End-to-end companion to the unit test above: a `new` expression
        // whose constructor path is split across lines must still register
        // in `collect_grpc_clients` and produce a resolved GRPC_CALL edge,
        // exactly like `extracts_grpc_js_impl_and_call`'s single-line form.
        let source = r#"
const grpc = require("@grpc/grpc-js");
const proto = { helloworld: { Greeter: { service: {} } } };
function sayHello(call, callback) {}
const server = new grpc.Server();
server.addService(proto.helloworld.Greeter.service, { sayHello });
const client = new proto.helloworld
    .Greeter("localhost:50051", grpc.credentials.createInsecure());
client.sayHello({ name: "world" }, () => {});
"#;
        let mut extractor = JavascriptExtractor::new().unwrap();
        let file = extractor.extract(source, "index").unwrap();
        let calls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_CALL_KIND)
            .collect::<Vec<_>>();
        assert!(
            calls.iter().any(|edge| {
                edge.target_qualname.as_deref() == Some("/helloworld.greeter/sayhello")
            }),
            "multi-line gRPC client receiver must still resolve to a GRPC_CALL edge, got {:?}",
            calls
        );
    }

    #[test]
    fn match_alias_pattern_extracts_wildcard_capture() {
        assert_eq!(
            match_alias_pattern("@/*", "@/lib/foo").as_deref(),
            Some("lib/foo")
        );
        assert_eq!(match_alias_pattern("@/*", "next/navigation"), None);
        assert_eq!(match_alias_pattern("@utils", "@utils").as_deref(), Some(""));
        assert_eq!(match_alias_pattern("@utils", "@utils/extra"), None);
        // More than one '*' isn't a shape tsconfig itself allows in a
        // pattern; refused rather than guessed at.
        assert_eq!(match_alias_pattern("@/*/*", "@/a/b"), None);
    }

    #[test]
    fn substitute_alias_target_requires_matching_wildcard_shape() {
        assert_eq!(
            substitute_alias_target("./*", "lib/foo", true).as_deref(),
            Some("./lib/foo")
        );
        assert_eq!(
            substitute_alias_target("./src/*", "lib/foo", true).as_deref(),
            Some("./src/lib/foo")
        );
        // A literal target can't disambiguate a wildcard capture: refused.
        assert_eq!(substitute_alias_target("./fixed", "lib/foo", true), None);
        // An exact (wildcard-free) pattern needs an exact target.
        assert_eq!(
            substitute_alias_target("./utils/index.ts", "", false).as_deref(),
            Some("./utils/index.ts")
        );
        assert_eq!(substitute_alias_target("./*", "", false), None);
    }

    #[test]
    fn strip_jsonc_removes_comments_and_trailing_commas_outside_strings() {
        let input = r#"{
  // leading comment
  "compilerOptions": {
    "paths": {
      "@/*": ["./*"], // trailing line comment
    },
    /* block
       comment */
    "baseUrl": ".",
  },
  "note": "a // not a comment and /* not a comment either",
}"#;
        let cleaned = strip_jsonc(input);
        let value: serde_json::Value =
            serde_json::from_str(&cleaned).expect("cleaned text must parse as plain JSON");
        assert_eq!(
            value["compilerOptions"]["paths"]["@/*"][0]
                .as_str()
                .unwrap(),
            "./*"
        );
        assert_eq!(value["compilerOptions"]["baseUrl"].as_str().unwrap(), ".");
        assert_eq!(
            value["note"].as_str().unwrap(),
            "a // not a comment and /* not a comment either"
        );
    }

    /// Source qualname of the single CALLS edge whose (module-qualified)
    /// target ends in `.target`.
    fn call_source(file: &crate::indexer::extract::ExtractedFile, target: &str) -> String {
        let suffix = format!(".{target}");
        let hits: Vec<_> = file
            .edges
            .iter()
            .filter(|e| {
                e.kind == "CALLS"
                    && e.target_qualname
                        .as_deref()
                        .is_some_and(|t| t.ends_with(&suffix))
            })
            .collect();
        assert_eq!(hits.len(), 1, "expected one CALLS -> {target}");
        hits[0].source_qualname.clone().unwrap()
    }

    #[test]
    fn const_arrow_calls_attribute_to_const_tsx() {
        let source = r#"
import { useState } from 'react';
const Lazy = dynamic(() => loadGraph(), { ssr: false });
export const ProductTabs: FC<Props> = ({ item }) => {
  const [tab, setTab] = useState('a');
  const onClick = () => track(tab);
  function inner() { return deep(); }
  return <Tabs value={tab}>{renderRows(item)}</Tabs>;
};
const { a } = pick();
setup();
"#;
        let mut extractor = super::TsxExtractor::new().unwrap();
        let file = extractor.extract(source, "components.tabs").unwrap();
        assert_eq!(
            call_source(&file, "useState"),
            "components.tabs.ProductTabs"
        );
        assert_eq!(
            call_source(&file, "renderRows"),
            "components.tabs.ProductTabs"
        );
        // A handler const nested inside the component stays the component's.
        assert_eq!(call_source(&file, "track"), "components.tabs.ProductTabs");
        // Nested named function: innermost wins.
        assert_eq!(call_source(&file, "deep"), "components.tabs.inner");
        // The HOC-style wrapper call itself is a module-level call ...
        assert_eq!(call_source(&file, "dynamic"), "components.tabs");
        // ... but the callback passed to it belongs to the const.
        assert_eq!(call_source(&file, "loadGraph"), "components.tabs.Lazy");
        // Destructuring and bare module-level calls stay on the module.
        assert_eq!(call_source(&file, "pick"), "components.tabs");
        assert_eq!(call_source(&file, "setup"), "components.tabs");
    }

    #[test]
    fn const_arrow_and_object_property_calls_attribute_to_const_ts() {
        let source = r#"
export const load = async (id: string): Promise<Item> => fetchItem(id);
export const apiClient = {
  get: <T>(endpoint: string) => apiClientFetch<T>(endpoint, { method: 'GET' }),
  post(endpoint: string, data?: unknown) { return apiPost(endpoint, data); },
};
const cfg = buildConfig();
"#;
        let mut extractor = super::TypescriptExtractor::new().unwrap();
        let file = extractor.extract(source, "lib.api").unwrap();
        assert_eq!(call_source(&file, "fetchItem"), "lib.api.load");
        assert_eq!(call_source(&file, "apiClientFetch"), "lib.api.apiClient");
        assert_eq!(call_source(&file, "apiPost"), "lib.api.apiClient");
        assert_eq!(call_source(&file, "buildConfig"), "lib.api");
    }

    #[test]
    fn const_function_expression_calls_attribute_to_const_js() {
        let source = r#"
const handler = function (req) { return process(req); };
var gen = function* () { yield step(); };
let arrow = x => transform(x);
module.exports = { handler };
init();
"#;
        let mut extractor = JavascriptExtractor::new().unwrap();
        let file = extractor.extract(source, "srv").unwrap();
        assert_eq!(call_source(&file, "process"), "srv.handler");
        assert_eq!(call_source(&file, "step"), "srv.gen");
        assert_eq!(call_source(&file, "transform"), "srv.arrow");
        assert_eq!(call_source(&file, "init"), "srv");
    }

    /// Issue #110: `new Error(...)` must never fuzzy-bind to an unrelated
    /// same-named symbol elsewhere in the index. Gated at extraction
    /// (`ReceiverType::Unresolved` -- "tracked but unresolved/builtin: no
    /// lookup attempted at all", same signal a builtin-typed receiver
    /// already gets), one call-graph layer before the DB resolver's own
    /// case-sensitivity fix (`db::resolver`) even gets a say.
    #[test]
    fn new_error_call_is_gated_from_fuzzy_resolution() {
        let source = r#"
function handler() {
    throw new Error('boom');
}
"#;
        let mut extractor = JavascriptExtractor::new().unwrap();
        let file = extractor.extract(source, "index").unwrap();
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert_eq!(calls.len(), 1, "{calls:?}");
        assert_eq!(calls[0].receiver_type, ReceiverType::Unresolved);
    }

    /// Same gate, a bare (non-`new`) call -- `Error(...)` without `new` is
    /// valid JS/TS and constructs an `Error` too.
    #[test]
    fn bare_error_call_is_gated_from_fuzzy_resolution() {
        let source = r#"
function handler() {
    return Error('boom');
}
"#;
        let mut extractor = JavascriptExtractor::new().unwrap();
        let file = extractor.extract(source, "index").unwrap();
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert_eq!(calls.len(), 1, "{calls:?}");
        assert_eq!(calls[0].receiver_type, ReceiverType::Unresolved);
    }

    /// The gate only fires for an *unshadowed* global name -- a repo
    /// import binding under the same name (however unusual) must resolve
    /// normally instead (see `is_unshadowed_global_callable`).
    #[test]
    fn import_shadowed_global_name_is_not_gated() {
        let source = r#"
import { Map } from './my-map';
function handler() {
    return new Map();
}
"#;
        let mut extractor = JavascriptExtractor::new().unwrap();
        let file = extractor.extract(source, "index").unwrap();
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert_eq!(calls.len(), 1, "{calls:?}");
        assert_eq!(calls[0].receiver_type, ReceiverType::NotTracked);
    }

    /// New global `URL` constructor is gated from fuzzy resolution.
    #[test]
    fn new_url_constructor_is_gated() {
        let source = r#"
function fetchFile(path) {
    const url = new URL(path, 'https://example.com');
    return url.href;
}
"#;
        let mut extractor = JavascriptExtractor::new().unwrap();
        let file = extractor.extract(source, "index").unwrap();
        let calls: Vec<_> = file.edges.iter().filter(|e| e.kind == "CALLS").collect();
        assert!(
            calls
                .iter()
                .any(|c| c.receiver_type == ReceiverType::Unresolved),
            "new URL(...) should be gated: {calls:?}"
        );
    }
}

#[cfg(test)]
mod import_resolution_tests {
    use crate::indexer::Indexer;
    use rusqlite::Connection;

    /// Index `files` (repo-relative path, source) as a fresh repo with a
    /// `@/*` tsconfig path alias and open the resulting DB.
    fn index_repo(files: &[(&str, &str)]) -> (tempfile::TempDir, Connection) {
        let dir = tempfile::TempDir::new().unwrap();
        let root = dir.path();
        std::fs::write(
            root.join("tsconfig.json"),
            r#"{ "compilerOptions": { "paths": { "@/*": ["./*"] } } }"#,
        )
        .unwrap();
        for (rel, source) in files {
            let path = root.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, source).unwrap();
        }
        let db_path = root.join(".lidx").join("db.sqlite");
        Indexer::new(root.to_path_buf(), db_path.clone())
            .unwrap()
            .reindex()
            .unwrap();
        let conn = Connection::open(db_path).unwrap();
        (dir, conn)
    }

    /// Resolved target qualname of the single CALLS edge from `caller`
    /// whose literal target ends in `.{name}`; `None` when it stays unbound.
    /// Issue #79: an unresolved CALLS edge is no longer written at all, so
    /// "stays unbound" now also means zero rows, not just a NULL target.
    fn callee(conn: &Connection, caller: &str, name: &str) -> Option<String> {
        let mut stmt = conn
            .prepare(
                "SELECT t.qualname FROM edges e
                 JOIN symbols s ON e.source_symbol_id = s.id
                 LEFT JOIN symbols t ON e.target_symbol_id = t.id
                 WHERE e.kind = 'CALLS' AND s.qualname = ?1
                   AND e.target_qualname LIKE '%.' || ?2",
            )
            .unwrap();
        let rows: Vec<Option<String>> = stmt
            .query_map([caller, name], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            rows.len() <= 1,
            "expected at most one CALLS {caller} -> {name}"
        );
        rows.into_iter().next().flatten()
    }

    const BUTTON: &str = r#"
import { cn } from '@/lib/utils';
import { cn as classNames } from '../lib/utils';
import * as api from '../lib/api';
import formatName from '@/lib/fmt';
import { useState } from 'react';
export function Button() {
  cn('a');
  classNames('b');
  api.get('x');
  formatName('y');
  useState(0);
  return null;
}
"#;

    fn button_repo() -> (tempfile::TempDir, Connection) {
        index_repo(&[
            (
                "lib/utils.ts",
                "export function cn(...a: string[]) { return a.join(' '); }\n",
            ),
            (
                "lib/api.ts",
                "export function get(p: string) { return p; }\n",
            ),
            (
                "lib/fmt.ts",
                "export default function formatName(n: string) { return n; }\n",
            ),
            // Decoy: the only repo symbol named `useState`, in the caller's
            // language family. The caller imports `useState` from `react`,
            // so it must never bind here.
            (
                "other/hooks.ts",
                "export function useState(x: number) { return x; }\n",
            ),
            ("components/button.tsx", BUTTON),
        ])
    }

    #[test]
    fn tsx_named_import_via_alias_binds_to_ts_export() {
        let (_dir, conn) = button_repo();
        assert_eq!(
            callee(&conn, "components/button.Button", "cn").as_deref(),
            Some("lib/utils.cn")
        );
    }

    #[test]
    fn tsx_renamed_relative_import_binds_to_ts_export() {
        let (_dir, conn) = button_repo();
        assert_eq!(
            callee(&conn, "components/button.Button", "classNames").as_deref(),
            Some("lib/utils.cn")
        );
    }

    #[test]
    fn esm_js_suffixed_import_binds_to_ts_source() {
        let (_dir, conn) = index_repo(&[
            (
                "src/schemas.ts",
                "export function compileSchema(s: string) { return s; }\n",
            ),
            (
                "src/main.ts",
                "import { compileSchema } from './schemas.js';\nexport function boot() { return compileSchema('x'); }\n",
            ),
        ]);
        assert_eq!(
            callee(&conn, "src/main.boot", "compileSchema").as_deref(),
            Some("src/schemas.compileSchema")
        );
    }

    #[test]
    fn tsx_namespace_import_member_call_binds_to_ts_export() {
        let (_dir, conn) = button_repo();
        assert_eq!(
            callee(&conn, "components/button.Button", "get").as_deref(),
            Some("lib/api.get")
        );
    }

    #[test]
    fn tsx_default_import_binds_to_ts_default_export() {
        let (_dir, conn) = button_repo();
        assert_eq!(
            callee(&conn, "components/button.Button", "formatName").as_deref(),
            Some("lib/fmt.formatName")
        );
    }

    #[test]
    fn local_binding_shadowing_an_import_gets_no_import_candidate() {
        use crate::indexer::extract::LanguageExtractor;
        let source = r#"
import { cn } from '@/lib/utils';
export function a(cn: (x: string) => string) { return cn('x'); }
export function b() { return cn('y'); }
"#;
        let mut extractor = super::TypescriptExtractor::new().unwrap();
        let file = extractor.extract(source, "m").unwrap();
        let candidates = |src: &str| {
            file.edges
                .iter()
                .find(|e| e.kind == "CALLS" && e.source_qualname.as_deref() == Some(src))
                .unwrap()
                .import_candidates
                .clone()
        };
        assert!(candidates("m.a").is_empty());
        assert_eq!(candidates("m.b").len(), 1);
    }

    #[test]
    fn external_package_import_never_binds_to_same_named_repo_symbol() {
        // Issue #80: `useState` (imported from `react`) binds to the
        // external stub instead of staying unresolved -- the load-bearing
        // check is still that it's never the decoy `other/hooks.useState`.
        let (_dir, conn) = button_repo();
        assert_eq!(
            callee(&conn, "components/button.Button", "useState").as_deref(),
            Some("ext:react:useState")
        );
    }
}
