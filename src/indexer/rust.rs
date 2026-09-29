use crate::db::resolver::{
    DeclarationIndex, DeclarationQuery, ImportMissPolicy, LanguageProfile, VisibilityRule,
};
use crate::indexer::channel;
use crate::indexer::config;
use crate::indexer::extract::{
    DeferredSource, EdgeInput, ExtractedFile, ReceiverType, RustDeferred, Step, SymbolInput,
};
use crate::indexer::http;
use crate::indexer::proto;
use crate::indexer::tree_helpers::{
    collapse_call_target_whitespace, module_symbol_fallback, module_symbol_with_span, node_text,
    span,
};
use crate::util;
use anyhow::Result;
use serde_json::json;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tree_sitter::{Node, Parser};

/// Rust's resolution profile. `use` targets are absolute after
/// `normalize_import_target`, so suffix matching is off.
pub(crate) const PROFILE: LanguageProfile = LanguageProfile {
    separators: &["::"],
    normalize_import_target: Some(normalize_import_target),
    import_miss: ImportMissPolicy::FallThrough,
    import_suffix_matching: false,
    import_member_fallback: false,
    visibility: VisibilityRule::RustModule,
    return_receiver: None,
    deferred_receiver: Some(resolve_deferred),
};

/// `LanguageProfile::normalize_import_target` for Rust: rewrite
/// `self::`/`super::` (including repeated `super::super::...`) relative to
/// `module`, the qualname of the module the path was written in. `crate::`
/// and anything else pass through unchanged (`None`: no rewrite needed).
///
/// Shared by every place this extractor turns a relative path into an
/// absolute qualname: `resolve_call_target` (call-site paths) and
/// `collect_use_bindings`/`handle_use` (`use self::x` / `use super::x`
/// targets), so a call site and a `use` statement rewrite the same way.
fn normalize_import_target(raw: &str, module: &str) -> Option<String> {
    if let Some(rest) = raw.strip_prefix("self::") {
        if rest.is_empty() {
            return None;
        }
        return Some(format!("{module}::{rest}"));
    }
    if raw.starts_with("super::") {
        let mut current = module;
        let mut rest = raw;
        while let Some(tail) = rest.strip_prefix("super::") {
            let (parent, _) = current.rsplit_once("::")?; // super:: past the crate root — no guess
            current = parent;
            rest = tail;
        }
        if rest.is_empty() {
            return None;
        }
        return Some(format!("{current}::{rest}"));
    }
    None
}

#[derive(Clone)]
struct Context {
    module: String,
    container_stack: Vec<String>,
    current_scope: String,
    grpc_service: Option<GrpcService>,
    grpc_clients: HashMap<String, GrpcService>,
    /// This *scope's own* `use` bindings: bound name -> fully-qualified
    /// target(s), consulted for the CALLS import tier when a bare call
    /// isn't a same-scope item. Recomputed fresh at the file root and at
    /// every nested `mod` (see `collect_use_bindings`) — never inherited
    /// from an enclosing module, matching Rust's own namespacing (a nested
    /// `mod` doesn't see its parent's `use`s, only `super::`/`crate::`
    /// qualified access).
    imports: Rc<HashMap<String, Vec<String>>>,
    /// Names the current function must not get an import candidate for
    /// (`collect_shadowed_names`); empty outside a function body.
    shadowed_names: Rc<HashSet<String>>,
    /// Set on entry to a trait declaration's own body (default methods)
    /// or a `impl Trait for Type` block's body (trait method
    /// implementations) — either way, the method's real visibility is the
    /// trait's own, not whatever `pub`/no-`pub` appears on the item
    /// itself, which Rust doesn't require or even always allow there. See
    /// `handle_function`'s use of it: it never records such a method as
    /// private (issue #75 follow-up, finding B).
    in_trait_scope: bool,
    /// Set only inside an `impl Trait for Type` body (not a trait declaration).
    in_trait_impl: Option<String>,
    /// Current function's locals/params whose type is locally knowable
    /// (`collect_local_types`), scoped to the block/arm that binds them.
    /// Empty outside a function body.
    local_types: Rc<Locals>,
    /// Same-file struct / enum-variant field types (`collect_adts`), used to
    /// type struct and tuple-struct patterns.
    adts: Rc<Adts>,
    /// Same-file fn/method return types (`collect_returns`).
    returns: Rc<Returns>,
}

pub struct RustExtractor {
    parser: Parser,
    /// Repo root for crate-root detection (issue #129), set via
    /// `with_repo_root`. `None` (the `new()` default, used by every
    /// standalone/unit-test extractor that never sees a real repo layout)
    /// keeps the old behavior: the whole repo is treated as one crate
    /// rooted at `repo_root`, matching lidx's own self-index (a single
    /// crate at the repo root).
    repo_root: Option<PathBuf>,
    /// Memoizes `find_crate_root`'s Cargo.toml walk per directory queried,
    /// since `module_name_from_rel_path` runs once per file.
    crate_root_cache: RefCell<HashMap<PathBuf, Option<PathBuf>>>,
}

impl RustExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_rust::LANGUAGE;
        parser.set_language(&language.into())?;
        Ok(Self {
            parser,
            repo_root: None,
            crate_root_cache: RefCell::new(HashMap::new()),
        })
    }

    /// Enables nested-crate-root detection (issue #129):
    /// `module_name_from_rel_path` walks up from each file's directory to
    /// the nearest ancestor containing `Cargo.toml` and treats that
    /// directory -- not `repo_root` -- as the crate root, so a crate nested
    /// several directories deep (e.g. `node/dpb-app/src-tauri`) gets
    /// qualnames relative to its own `src/` instead of ones that embed the
    /// full repo path (`crate::node::dpb-app::src-tauri::src::...`).
    pub fn with_repo_root(mut self, repo_root: PathBuf) -> Self {
        self.repo_root = Some(repo_root);
        self
    }
}

impl crate::indexer::extract::LanguageExtractor for RustExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String {
        let Some(repo_root) = self.repo_root.as_deref() else {
            return module_name_from_rel_path(rel_path);
        };
        let dir = Path::new(rel_path)
            .parent()
            .unwrap_or_else(|| Path::new(""));
        let crate_root = find_crate_root(repo_root, dir, &self.crate_root_cache);
        module_name_from_rel_path(&strip_crate_root(rel_path, crate_root.as_deref()))
    }

    fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile> {
        let mut output = ExtractedFile::default();
        let tree = match self.parser.parse(source, None) {
            Some(tree) => tree,
            None => {
                output
                    .symbols
                    .push(module_symbol_fallback(module_name, source, "::", None));
                return Ok(output);
            }
        };
        let root = tree.root_node();

        let module_span = span(root);
        output.symbols.push(module_symbol_with_span(
            module_name,
            module_span,
            "::",
            None,
        ));
        let ctx = Context {
            module: module_name.to_string(),
            container_stack: Vec::new(),
            current_scope: module_name.to_string(),
            grpc_service: None,
            grpc_clients: HashMap::new(),
            imports: Rc::new(collect_use_bindings(root, source, module_name)),
            shadowed_names: Rc::new(HashSet::new()),
            local_types: Rc::new(HashMap::new()),
            adts: Rc::new(collect_adts(root, source)),
            returns: Rc::new(collect_returns(root, source)),
            in_trait_scope: false,
            in_trait_impl: None,
        };
        walk_node(root, &ctx, source, &mut output);
        collect_uses(root, &ctx, source, &mut output, &mut HashSet::new());
        Ok(output)
    }

    fn resolve_imports(
        &self,
        repo_root: &Path,
        file_rel_path: &str,
        module_name: &str,
        edges: &mut Vec<crate::indexer::extract::EdgeInput>,
    ) {
        resolve_module_file_edges(repo_root, file_rel_path, module_name, edges);
    }
}

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "crate".to_string();
    }
    if parts.first().map(|part| part == "src").unwrap_or(false) {
        parts.remove(0);
    }
    let file = parts.pop().unwrap_or_default();
    let stem = Path::new(&file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(&file)
        .to_string();
    match stem.as_str() {
        "lib" | "main" => {}
        "mod" => {}
        _ => parts.push(stem),
    }
    if parts.is_empty() {
        "crate".to_string()
    } else {
        format!("crate::{}", parts.join("::"))
    }
}

/// The nearest ancestor of `start_dir` (inclusive), relative to
/// `repo_root`, that contains a `Cargo.toml` -- the crate root for any file
/// under it (issue #129). Returns `None` when no ancestor up to and including
/// `repo_root` has one, which reproduces the pre-#129 single-crate-at-repo-root
/// behavior exactly (also lidx's own self-index).
///
/// Memoizes every directory visited during the walk in `cache`, not just
/// `start_dir` itself, so a later query for a sibling directory (a
/// different file in the same crate) usually resolves in one cache lookup
/// rather than re-walking to the crate root again.
fn find_crate_root(
    repo_root: &Path,
    start_dir: &Path,
    cache: &RefCell<HashMap<PathBuf, Option<PathBuf>>>,
) -> Option<PathBuf> {
    let mut visited = Vec::new();
    let mut current = start_dir.to_path_buf();
    let found = loop {
        if let Some(hit) = cache.borrow().get(&current) {
            break hit.clone();
        }
        visited.push(current.clone());
        if repo_root.join(&current).join("Cargo.toml").is_file() {
            break Some(current.clone());
        }
        if !current.pop() {
            // Walked past the repo root without finding a Cargo.toml
            // anywhere: no crate root to report.
            break None;
        }
    };
    let mut cache = cache.borrow_mut();
    for dir in visited {
        cache.insert(dir, found.clone());
    }
    found
}

/// `rel_path` with `crate_root`'s components stripped from the front, as a
/// `/`-joined string ready for `module_name_from_rel_path`. When `crate_root`
/// is `None` (no Cargo.toml found, see `find_crate_root`), strips nothing.
fn strip_crate_root(rel_path: &str, crate_root: Option<&Path>) -> String {
    match crate_root {
        Some(root) => match Path::new(rel_path).strip_prefix(root) {
            Ok(rest) => rest
                .components()
                .filter_map(|comp| comp.as_os_str().to_str())
                .collect::<Vec<_>>()
                .join("/"),
            Err(_) => rel_path.to_string(),
        },
        None => rel_path.to_string(),
    }
}

fn walk_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if node.kind() == "source_file" || node.kind() == "declaration_list" {
        walk_declaration_list(node, ctx, source, output);
        return;
    }
    if node.kind() == "call_expression" {
        handle_call(node, ctx, source, output);
    }
    match node.kind() {
        "mod_item" => {
            handle_mod(node, ctx, source, output);
            return;
        }
        "struct_item" => {
            handle_named_item(node, ctx, source, output, "struct");
            return;
        }
        "enum_item" => {
            handle_named_item(node, ctx, source, output, "enum");
            return;
        }
        "trait_item" => {
            handle_trait(node, ctx, source, output);
            return;
        }
        "type_item" => {
            if ctx.container_stack.is_empty() {
                handle_named_item(node, ctx, source, output, "type");
            }
            return;
        }
        "const_item" => {
            if ctx.container_stack.is_empty() {
                handle_named_item(node, ctx, source, output, "const");
            }
            return;
        }
        "static_item" => {
            if ctx.container_stack.is_empty() {
                handle_named_item(node, ctx, source, output, "static");
            }
            return;
        }
        "function_item" => {
            // Reached directly (not via `walk_declaration_list`'s
            // attribute-tracking loop), so no preceding `attribute_item`s
            // are associated with it -- this only happens for a
            // `function_item` nested somewhere other than a
            // `source_file`/`declaration_list` (e.g. a fn nested inside a
            // block), where `#[test]` wouldn't apply anyway.
            handle_function(node, ctx, source, output, &[]);
            return;
        }
        "function_signature_item" => {
            handle_function_signature(node, ctx, source, output);
            return;
        }
        "use_declaration" | "use_item" => {
            if ctx.container_stack.is_empty() {
                handle_use(node, ctx, source, output);
            }
            return;
        }
        "impl_item" => {
            handle_impl(node, ctx, source, output);
            return;
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

fn walk_declaration_list(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut pending_attrs: Vec<Node<'_>> = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "attribute_item" {
            pending_attrs.push(child);
            continue;
        }
        if child.kind() == "function_item" {
            handle_function_with_attributes(child, ctx, source, output, &pending_attrs);
            pending_attrs.clear();
            continue;
        }
        if !pending_attrs.is_empty() {
            pending_attrs.clear();
        }
        walk_node(child, ctx, source, output);
    }
}

fn handle_named_item(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    kind: &str,
) {
    let Some(name) = extract_name(node, source) else {
        return;
    };
    let qualname = format!("{}::{}", ctx.module, name);
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
        signature: if matches!(kind, "struct" | "enum") {
            adt_signature(node, source)
        } else {
            None
        },
        docstring: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(ctx.module.clone()),
        target_qualname: Some(qualname),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
}

fn handle_trait(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name) = extract_name(node, source) else {
        return;
    };
    let qualname = format!("{}::{}", ctx.module, name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    output.symbols.push(SymbolInput {
        kind: "trait".to_string(),
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
    next_ctx.container_stack.push(qualname);
    // A default method declared directly in the trait is exactly as
    // visible as the trait itself, regardless of its own (often absent)
    // `pub` — see `Context::in_trait_scope`.
    next_ctx.in_trait_scope = true;
    if let Some(body) = body_node(node) {
        walk_node(body, &next_ctx, source, output);
    }
}

fn handle_mod(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name) = extract_name(node, source) else {
        return;
    };
    let module_name = format!("{}::{}", ctx.module, name);
    let Some(body) = body_node(node) else {
        let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
        let snippet =
            util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
        output.edges.push(EdgeInput {
            kind: "MODULE_FILE".to_string(),
            source_qualname: Some(ctx.module.clone()),
            target_qualname: Some(module_name),
            detail: None,
            evidence_snippet: snippet,
            evidence_start_line: Some(start_line),
            evidence_end_line: Some(end_line),
            ..Default::default()
        });
        return;
    };
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    output.symbols.push(SymbolInput {
        kind: "module".to_string(),
        name: name.clone(),
        qualname: module_name.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature: None,
        docstring: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(ctx.module.clone()),
        target_qualname: Some(module_name.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    let mut next_ctx = ctx.clone();
    next_ctx.module = module_name;
    next_ctx.current_scope = next_ctx.module.clone();
    // Fresh, not merged with `ctx.imports`: this module doesn't inherit its
    // parent's `use` bindings (see `Context::imports`).
    next_ctx.imports = Rc::new(collect_use_bindings(body, source, &next_ctx.module));
    // A module is its own namespace, not a function body: no shadowed
    // names carry in, even for a `mod` declared inside a function.
    next_ctx.shadowed_names = Rc::new(HashSet::new());
    next_ctx.local_types = Rc::new(HashMap::new());
    walk_node(body, &next_ctx, source, output);
}

pub fn resolve_module_file_edges(
    repo_root: &Path,
    file_rel_path: &str,
    file_module: &str,
    edges: &mut [EdgeInput],
) {
    let base_dir = module_base_dir(file_rel_path);
    for edge in edges {
        if edge.kind != "MODULE_FILE" {
            continue;
        }
        let (source_module, target_qualname) = match (
            edge.source_qualname.as_deref(),
            edge.target_qualname.as_deref(),
        ) {
            (Some(source), Some(target)) => (source, target),
            _ => continue,
        };
        let dst_name = target_qualname
            .rsplit("::")
            .next()
            .unwrap_or(target_qualname)
            .to_string();

        let mut dst_path = None;
        let mut confidence = 0.4;
        if let Some(source_dir) = module_dir_for_source(file_module, source_module, &base_dir) {
            let candidate_rs = source_dir.join(format!("{dst_name}.rs"));
            let candidate_mod = source_dir.join(&dst_name).join("mod.rs");
            if repo_root.join(&candidate_rs).is_file() {
                dst_path = Some(util::normalize_path(&candidate_rs));
                confidence = 1.0;
            } else if repo_root.join(&candidate_mod).is_file() {
                dst_path = Some(util::normalize_path(&candidate_mod));
                confidence = 1.0;
            }
        }

        edge.detail = Some(
            json!({
                "src_path": file_rel_path,
                "dst_path": dst_path,
                "dst_name": dst_name,
                "confidence": confidence,
            })
            .to_string(),
        );
    }
}

fn module_base_dir(rel_path: &str) -> std::path::PathBuf {
    let path = Path::new(rel_path);
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    if matches!(stem, "lib" | "main" | "mod") || stem.is_empty() {
        parent.to_path_buf()
    } else {
        parent.join(stem)
    }
}

fn module_dir_for_source(
    file_module: &str,
    source_module: &str,
    base_dir: &Path,
) -> Option<std::path::PathBuf> {
    if source_module == file_module {
        return Some(base_dir.to_path_buf());
    }
    let prefix = format!("{file_module}::");
    let rest = source_module.strip_prefix(&prefix)?;
    let mut dir = base_dir.to_path_buf();
    if rest.is_empty() {
        return Some(dir);
    }
    for segment in rest.split("::") {
        if segment.is_empty() {
            continue;
        }
        dir.push(segment);
    }
    Some(dir)
}

fn handle_function(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    attributes: &[Node<'_>],
) {
    let Some(name) = extract_name(node, source) else {
        return;
    };
    let (qualname, parent, kind) = match ctx.container_stack.last() {
        Some(container) => (format!("{container}::{name}"), container.clone(), "method"),
        None => (
            format!("{}::{}", ctx.module, name),
            ctx.module.clone(),
            "function",
        ),
    };
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = extract_signature(node, source, attributes);
    // A trait default method or trait-impl method has no `pub` to check —
    // it's exactly as visible as the trait itself (see
    // `Context::in_trait_scope`, issue #75 follow-up, finding B).
    if !ctx.in_trait_scope && !has_pub_visibility(node) {
        output.private_qualnames.push(qualname.clone());
    }
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
        signature,
        docstring: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(parent),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
    if let Some(trait_qualname) = &ctx.in_trait_impl {
        // Reached through its trait (often external), so `dead_symbols`
        // must not report it.
        output.edges.push(EdgeInput {
            kind: "IMPLEMENTS".to_string(),
            source_qualname: Some(qualname.clone()),
            target_qualname: Some(format!("{trait_qualname}::{name}")),
            ..Default::default()
        });
    }
    if let Some(edge) = grpc_impl_edge(node, ctx, source, &name, &qualname) {
        output.edges.push(edge);
    }
    if let Some(body) = node.child_by_field_name("body") {
        let mut next_ctx = ctx.clone();
        next_ctx.current_scope = qualname;
        let mut grpc_clients = ctx.grpc_clients.clone();
        grpc_clients.extend(collect_grpc_clients(body, source, &ctx.imports));
        next_ctx.grpc_clients = grpc_clients;

        // Recomputed per function, never inherited.
        let mut shadowed = HashSet::new();
        collect_shadowed_names(node, source, &mut shadowed);
        next_ctx.shadowed_names = Rc::new(shadowed);

        // In a trait declaration `Self` is not the trait.
        let self_ty = ctx
            .container_stack
            .last()
            .filter(|_| !(ctx.in_trait_scope && ctx.in_trait_impl.is_none()))
            .map(|c| c.rsplit("::").next().unwrap_or(c).to_string());
        let mut local_types = HashMap::new();
        let env = TypeEnv {
            source,
            self_ty: self_ty.as_deref(),
            generics: collect_generic_names(node, source),
            adts: &ctx.adts,
            ctx: Some(&next_ctx),
        };
        collect_local_types(node, &env, &mut local_types);
        next_ctx.local_types = Rc::new(local_types);

        walk_node(body, &next_ctx, source, output);
    }
}

fn handle_function_with_attributes(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    attributes: &[Node<'_>],
) {
    let Some(name) = extract_name(node, source) else {
        return;
    };
    let qualname = match ctx.container_stack.last() {
        Some(container) => format!("{container}::{name}"),
        None => format!("{}::{}", ctx.module, name),
    };
    for edge in route_edges_from_attribute_items(attributes, ctx, source, &qualname) {
        output.edges.push(edge);
    }
    handle_function(node, ctx, source, output, attributes);
}

fn handle_function_signature(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let Some(name) = extract_name(node, source) else {
        return;
    };
    let Some(container) = ctx.container_stack.last() else {
        return;
    };
    let qualname = format!("{container}::{name}");
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = extract_signature(node, source, &[]);
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
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(container.clone()),
        target_qualname: Some(qualname),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
}

fn handle_impl(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(type_node) = node.child_by_field_name("type") else {
        return;
    };
    let type_name = normalize_type_path(&node_text(type_node, source));
    if type_name.is_empty() {
        return;
    }
    let type_qualname = qualify_type_name(&ctx.module, &type_name);
    let is_trait_impl = node.child_by_field_name("trait").is_some();

    let mut grpc_service = None;
    if let Some(trait_node) = node.child_by_field_name("trait") {
        let trait_name = normalize_type_path(&node_text(trait_node, source));
        if !trait_name.is_empty() {
            let trait_qualname = qualify_type_name(&ctx.module, &trait_name);
            output.edges.push(EdgeInput {
                kind: "IMPLEMENTS".to_string(),
                source_qualname: Some(type_qualname.clone()),
                target_qualname: Some(trait_qualname),
                detail: None,
                evidence_snippet: None,
                ..Default::default()
            });
            grpc_service = grpc_service_from_trait(&trait_name);
        }
    }

    let body = match body_node(node) {
        Some(body) => body,
        None => return,
    };
    let mut next_ctx = ctx.clone();
    next_ctx.container_stack.push(type_qualname);
    next_ctx.grpc_service = grpc_service;
    // A trait impl's methods are exactly as visible as the trait itself —
    // Rust doesn't attach (and often doesn't allow) `pub` to them
    // directly — but an inherent impl's methods keep their own `pub`/
    // private status as normal. See `Context::in_trait_scope`.
    next_ctx.in_trait_scope = is_trait_impl;
    next_ctx.in_trait_impl = node
        .child_by_field_name("trait")
        .map(|t| qualify_type_name(&ctx.module, &normalize_type_path(&node_text(t, source))));
    walk_node(body, &next_ctx, source, output);
}

fn handle_use(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let text = node_text(node, source);
    for (_, raw_target) in parse_use_bindings(&text) {
        let target = normalized_import_target(raw_target, &ctx.module);
        output.edges.push(EdgeInput {
            kind: "IMPORTS".to_string(),
            source_qualname: Some(ctx.module.clone()),
            target_qualname: Some(target),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }
}

fn handle_call(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    for edge in http_route_edges(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = http_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = grpc_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = channel_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = config_read_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    let Some(function_node) = node.child_by_field_name("function") else {
        return;
    };
    let raw = node_text(function_node, source);
    if raw.is_empty() {
        return;
    }
    // Collapsed once here so a multi-line chain feeds both tiers below the
    // same shape the single-line form would.
    let collapsed = collapse_call_target_whitespace(&raw);
    let import_candidates = import_qualified_candidates(&collapsed, ctx);
    // When full resolution fails for a dotted call (e.g. `db.insert(...)`),
    // emit the bare method name as target_qualname rather than dropping the
    // target, giving Rust the same reach as Python, C#, and Go. Fully-resolved
    // targets are left untouched.
    let target = resolve_call_target(&collapsed, ctx).or_else(|| {
        call_target_parts(function_node, source)
            .filter(|parts| parts.receiver.is_some())
            .map(|parts| parts.name)
            .filter(|name| !name.is_empty())
    });
    let detail = if target.is_some() { None } else { Some(raw) };
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    output.edges.push(EdgeInput {
        kind: "CALLS".to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: target,
        import_candidates,
        detail,
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        // A bare identifier callee (`foo()`) vs. anything else
        // (`self.foo()`, `Type::method()`, `obj.foo()`) — see
        // `EdgeInput::bare_call`'s doc.
        bare_call: function_node.kind() == "identifier",
        receiver_type: infer_receiver_type(function_node, source, ctx),
        ..Default::default()
    });
}

/// `x.method()` where `x` is a plain local/param with a locally-inferred
/// type -> `Known(T)`, so the resolver binds `T::method` instead of
/// refusing an ambiguous bare name. Anything else stays `NotTracked`.
fn infer_receiver_type(function_node: Node<'_>, source: &str, ctx: &Context) -> ReceiverType {
    if function_node.kind() != "field_expression" {
        return ReceiverType::NotTracked;
    }
    let Some(value) = function_node.child_by_field_name("value") else {
        return ReceiverType::NotTracked;
    };
    if value.kind() != "identifier" {
        return ReceiverType::NotTracked;
    }
    match lookup_local(
        &ctx.local_types,
        &node_text(value, source),
        value.start_byte(),
    ) {
        Ty::Named(ty) => ReceiverType::Known(ty),
        Ty::Pending(p) => ReceiverType::Deferred(p.encode()),
        _ => ReceiverType::NotTracked,
    }
}

/// Std wrappers whose method calls mostly dispatch to the wrapped type
/// (auto-deref), so the outer name is not a usable receiver type.
const TRANSPARENT_WRAPPERS: &[&str] = &[
    "Arc", "Rc", "Box", "Cow", "Option", "Result", "Vec", "RefCell", "Cell", "Mutex", "RwLock",
];

/// Locally-inferred type of a value. `Unknown` is the "never wrong" escape
/// hatch: anything not provable from the current function or same-file item
/// declarations. Only `Named` can become a receiver type.
#[derive(Clone, Debug, PartialEq)]
enum Ty {
    Unknown,
    Named(String),
    Option(Box<Ty>),
    Result(Box<Ty>, Box<Ty>),
    Tuple(Vec<Ty>),
    /// `Vec<T>`, `VecDeque<T>`, `[T]`, `[T; N]`.
    Seq(Box<Ty>),
    /// An iterator whose item type is known.
    Iter(Box<Ty>),
    /// Output of an `async fn`, awaited via `.await`.
    Future(Box<Ty>),
    /// Type known only from a declaration in another file; the resolver
    /// finishes it.
    Pending(RustDeferred),
}

/// Method names `project` follows on a `Pending` type.
const TRACKED_METHODS: &[&str] = &[
    "unwrap",
    "expect",
    "ok",
    "err",
    "as_ref",
    "as_mut",
    "clone",
    "take",
    "cloned",
    "copied",
    "iter",
    "into_iter",
    "iter_mut",
    "drain",
    "by_ref",
    "next",
    "next_back",
    "last",
    "pop",
    "pop_front",
    "pop_back",
    "first",
];

/// Apply one projection step; `Unknown` when the shape doesn't fit.
fn project(ty: Ty, step: Step) -> Ty {
    if let Ty::Pending(mut p) = ty {
        if matches!(&step, Step::Method(m) if !TRACKED_METHODS.contains(&m.as_str())) {
            return Ty::Unknown;
        }
        let keeps_fallback = matches!(&step, Step::Await)
            || matches!(&step, Step::Method(m) if m == "unwrap" || m == "expect");
        if !keeps_fallback {
            p.fallback = None;
        }
        p.steps.push(step);
        return Ty::Pending(p);
    }
    match (ty, step) {
        (Ty::Option(t), Step::OptionSome)
        | (Ty::Result(t, _), Step::ResultOk)
        | (Ty::Result(_, t), Step::ResultErr)
        | (Ty::Seq(t) | Ty::Iter(t), Step::Elem)
        | (Ty::Future(t), Step::Await) => *t,
        (Ty::Tuple(ts), Step::Tuple(i)) => ts.into_iter().nth(i).unwrap_or(Ty::Unknown),
        (ty, Step::Method(m)) => method_ty(ty, &m),
        _ => Ty::Unknown,
    }
}

/// Every tuple/`..`-free element list must line up positionally.
fn has_rest(elems: &[Node<'_>]) -> bool {
    elems.iter().any(|e| e.kind() == "remaining_field_pattern")
}

/// `Type` (struct) or `Enum::Variant` -> field types by name (`"0"`, `"1"`
/// for tuple fields). `None` when the key is declared more than once.
type Adts = HashMap<String, Option<Vec<(String, Ty)>>>;

/// Byte range a binding is visible in.
type Scope = (usize, usize);

struct Binding {
    scope: Scope,
    ty: Ty,
}
type Locals = HashMap<String, Vec<Binding>>;

/// Return types of this file's free fns (`name`) and methods
/// (`Type::name`); `None` when declared more than once with different types.
type Returns = HashMap<String, Option<Ty>>;

/// Type visible for `name` at byte `pos`: the innermost (latest-starting)
/// binding whose scope contains `pos`. Same-start bindings that disagree
/// (or-pattern alternatives) are `Unknown`.
fn lookup_local(locals: &Locals, name: &str, pos: usize) -> Ty {
    let mut best: Option<(usize, Ty)> = None;
    for b in locals.get(name).into_iter().flatten() {
        let (start, end) = b.scope;
        if start > pos || pos >= end {
            continue;
        }
        match &mut best {
            Some((best_start, ty)) if *best_start == start => {
                if *ty != b.ty {
                    *ty = Ty::Unknown;
                }
            }
            Some((best_start, _)) if *best_start > start => {}
            _ => best = Some((start, b.ty.clone())),
        }
    }
    best.map_or(Ty::Unknown, |(_, ty)| ty)
}

fn record_local(out: &mut Locals, name: String, ty: Ty, scope: Scope) {
    out.entry(name).or_default().push(Binding { scope, ty });
}

/// What `collect_local_types` needs to turn a type name into a receiver type.
struct TypeEnv<'a> {
    source: &'a str,
    self_ty: Option<&'a str>,
    /// Generic parameter names of the enclosing fn and impl/trait: a
    /// receiver of type `T` says nothing about which type declares the method.
    generics: HashSet<String>,
    adts: &'a Adts,
    /// Imports, shadowed names and same-file return types; absent when only
    /// declaration types are read (`collect_adts`, `project_deferred`).
    ctx: Option<&'a Context>,
}

impl TypeEnv<'_> {
    /// Accepts only nominal, capitalised, non-wrapper, non-generic names;
    /// `Self` maps to the enclosing impl type.
    fn usable(&self, name: String) -> Option<String> {
        let name = if name == "Self" {
            self.self_ty?.to_string()
        } else {
            name
        };
        (name.chars().next()?.is_uppercase()
            && !TRANSPARENT_WRAPPERS.contains(&name.as_str())
            && !self.generics.contains(&name))
        .then_some(name)
    }

    /// Bare type name of a type node: peels `&`/`&mut`, generics and paths
    /// (`&mut crate::a::Resolver<'_>` -> `Resolver`).
    fn type_name(&self, node: Node<'_>) -> Option<String> {
        let mut current = node;
        loop {
            match current.kind() {
                "reference_type" | "generic_type" => {
                    current = current.child_by_field_name("type")?
                }
                "scoped_type_identifier" => current = current.child_by_field_name("name")?,
                "type_identifier" => break,
                _ => return None,
            }
        }
        self.usable(node_text(current, self.source))
    }

    /// Structured type of a type node (`Option<T>`, `Result<T, E>`, tuples,
    /// sequences, iterators of known item type); anything else is `Named`
    /// via `type_name` or `Unknown`.
    fn ty(&self, node: Node<'_>) -> Ty {
        match node.kind() {
            "reference_type" => node
                .child_by_field_name("type")
                .map_or(Ty::Unknown, |t| self.ty(t)),
            "tuple_type" => {
                let mut cursor = node.walk();
                Ty::Tuple(
                    node.named_children(&mut cursor)
                        .map(|c| self.ty(c))
                        .collect(),
                )
            }
            "array_type" => node
                .child_by_field_name("element")
                .map_or(Ty::Unknown, |t| Ty::Seq(Box::new(self.ty(t)))),
            "abstract_type" => node
                .child_by_field_name("trait")
                .and_then(|t| self.iterator_item(t))
                .map_or(Ty::Unknown, |t| Ty::Iter(Box::new(t))),
            "generic_type" => {
                let name = node
                    .child_by_field_name("type")
                    .and_then(|t| match t.kind() {
                        "type_identifier" => Some(node_text(t, self.source)),
                        "scoped_type_identifier" => t
                            .child_by_field_name("name")
                            .map(|n| node_text(n, self.source)),
                        _ => None,
                    });
                let mut args = Vec::new();
                if let Some(list) = node.child_by_field_name("type_arguments") {
                    let mut cursor = list.walk();
                    args.extend(
                        list.named_children(&mut cursor)
                            .filter(|c| c.kind() != "lifetime"),
                    );
                }
                let arg = |i: usize| args.get(i).map_or(Ty::Unknown, |a| self.ty(*a));
                match (name.as_deref(), args.len()) {
                    // Method calls auto-deref, so for receiver purposes a
                    // smart pointer to `T` is a `T`.
                    (Some("Box" | "Arc" | "Rc"), 1) => arg(0),
                    (Some("Option"), 1) => Ty::Option(Box::new(arg(0))),
                    (Some("Result"), 1) => Ty::Result(Box::new(arg(0)), Box::new(Ty::Unknown)),
                    (Some("Result"), 2) => Ty::Result(Box::new(arg(0)), Box::new(arg(1))),
                    (Some("Vec" | "VecDeque"), 1) => Ty::Seq(Box::new(arg(0))),
                    (Some("IntoIter" | "Iter" | "IterMut" | "Drain"), 1) => {
                        Ty::Iter(Box::new(arg(0)))
                    }
                    _ => self.type_name(node).map_or(Ty::Unknown, Ty::Named),
                }
            }
            _ => self.type_name(node).map_or(Ty::Unknown, Ty::Named),
        }
    }

    /// `Iterator<Item = T>` -> `T`.
    fn iterator_item(&self, node: Node<'_>) -> Option<Ty> {
        if node.kind() != "generic_type" {
            return None;
        }
        let list = node.child_by_field_name("type_arguments")?;
        let mut cursor = list.walk();
        let binding = list.named_children(&mut cursor).find(|c| {
            c.kind() == "type_binding"
                && c.child_by_field_name("name")
                    .is_some_and(|n| node_text(n, self.source) == "Item")
        })?;
        Some(self.ty(binding.child_by_field_name("type")?))
    }

    /// Type of an expression: `T::new(..)`/`T::default()`/`T::with_*`/
    /// `T::from*`, `T { .. }`, `Some(..)`/`Ok(..)`/`Err(..)`, tuples, locals
    /// with a known type, and `?`/`.unwrap()`/`.expect(..)`/`.next()`/
    /// `.iter()`-style method chains over those.
    fn expr_ty(&self, value: Node<'_>, locals: &Locals) -> Ty {
        self.expr_ty_opt(value, locals).unwrap_or(Ty::Unknown)
    }

    fn expr_ty_opt(&self, value: Node<'_>, locals: &Locals) -> Option<Ty> {
        let source = self.source;
        match value.kind() {
            "parenthesized_expression" | "reference_expression" => {
                let inner = value
                    .child_by_field_name("value")
                    .or_else(|| value.named_child(0))?;
                self.expr_ty_opt(inner, locals)
            }
            // Only deref: `-x` / `!x` say nothing about the type.
            "unary_expression" if node_text(value, source).starts_with('*') => {
                self.expr_ty_opt(value.named_child(0)?, locals)
            }
            "self" => self.usable("Self".to_string()).map(Ty::Named),
            "identifier" => Some(lookup_local(
                locals,
                &node_text(value, source),
                value.start_byte(),
            )),
            "try_expression" => {
                let inner = self.expr_ty_opt(value.named_child(0)?, locals)?;
                Some(match inner {
                    Ty::Option(t) | Ty::Result(t, _) => *t,
                    Ty::Named(n) => Ty::Named(n),
                    Ty::Pending(_) => project(inner, Step::Method("unwrap".into())),
                    _ => Ty::Unknown,
                })
            }
            "await_expression" => {
                let inner = self.expr_ty_opt(value.named_child(0)?, locals)?;
                Some(project(inner, Step::Await))
            }
            "field_expression" => {
                let base = self.expr_ty_opt(value.child_by_field_name("value")?, locals)?;
                let field = node_text(value.child_by_field_name("field")?, source);
                Some(self.field_ty(base, &field))
            }
            "tuple_expression" => {
                let mut cursor = value.walk();
                Some(Ty::Tuple(
                    value
                        .named_children(&mut cursor)
                        .map(|c| self.expr_ty(c, locals))
                        .collect(),
                ))
            }
            "struct_expression" => {
                let name = self.type_name(value.child_by_field_name("name")?)?;
                Some(Ty::Named(name))
            }
            "call_expression" => {
                let function = value.child_by_field_name("function")?;
                match function.kind() {
                    "field_expression" => {
                        let recv =
                            self.expr_ty_opt(function.child_by_field_name("value")?, locals)?;
                        let method = node_text(function.child_by_field_name("field")?, source);
                        Some(self.method_call_ty(recv, &method))
                    }
                    "identifier" | "scoped_identifier" => {
                        self.path_call_ty(value, function, locals)
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    /// `recv.method(..)`: this file's `Type::method` return type, else the
    /// handful of std shapes, else (a `Named` receiver) the method's
    /// declaration in another file.
    fn method_call_ty(&self, recv: Ty, method: &str) -> Ty {
        if let Ty::Named(t) = &recv {
            if let Some(entry) = self
                .ctx
                .and_then(|c| c.returns.get(&format!("{t}::{method}")))
            {
                return entry.clone().unwrap_or(Ty::Unknown);
            }
            if matches!(method, "unwrap" | "expect") {
                return recv;
            }
            return self.pending(
                DeferredSource::Method {
                    receiver_type: t.clone(),
                    method: method.to_string(),
                },
                None,
            );
        }
        project(recv, Step::Method(method.to_string()))
    }

    fn pending(&self, source: DeferredSource, fallback: Option<String>) -> Ty {
        if self.ctx.is_none() {
            return fallback.map_or(Ty::Unknown, Ty::Named);
        }
        Ty::Pending(RustDeferred {
            source,
            steps: Vec::new(),
            fallback,
        })
    }

    /// Field `field` of a value of type `base`.
    fn field_ty(&self, base: Ty, field: &str) -> Ty {
        let is_index = field.chars().all(|c| c.is_ascii_digit());
        match base {
            Ty::Named(owner) => self.adt_field(&owner, None, field),
            Ty::Tuple(_) | Ty::Pending(_) if is_index => {
                project(base, Step::Tuple(field.parse().unwrap_or(usize::MAX)))
            }
            _ => Ty::Unknown,
        }
    }

    /// Field type of struct `owner` (`variant == None`) or enum variant
    /// `owner::variant`: from this file's declaration, or deferred to the
    /// declaration in another file.
    fn adt_field(&self, owner: &str, variant: Option<&str>, field: &str) -> Ty {
        if self.adts.contains_key(owner) {
            let key = variant.map_or_else(|| owner.to_string(), |v| format!("{owner}::{v}"));
            return match self.adts.get(&key) {
                Some(Some(fields)) => fields
                    .iter()
                    .find(|(name, _)| name == field)
                    .map_or(Ty::Unknown, |(_, ty)| ty.clone()),
                _ => Ty::Unknown,
            };
        }
        let field = variant.map_or_else(|| field.to_string(), |v| format!("{v}::{field}"));
        self.pending(
            DeferredSource::Field {
                owner: owner.to_string(),
                field,
            },
            None,
        )
    }

    /// `f(..)`, `Type::f(..)`, `Self::f(..)`, `module::f(..)`: this file's
    /// declared return type first, then the callee's declaration in another
    /// file (via `use` bindings / absolute paths), then -- constructor-named
    /// calls only -- the assumption that `T::new()` is a `T`.
    fn path_call_ty(&self, call: Node<'_>, function: Node<'_>, locals: &Locals) -> Option<Ty> {
        let source = self.source;
        let raw = collapse_call_target_whitespace(&node_text(function, source));
        if function.kind() == "identifier" {
            let args = call_arguments(call);
            if let [arg] = args.as_slice() {
                let wrap = |t: Ty| Box::new(t);
                match raw.as_str() {
                    "Some" => return Some(Ty::Option(wrap(self.expr_ty(*arg, locals)))),
                    "Ok" => {
                        let ok = wrap(self.expr_ty(*arg, locals));
                        return Some(Ty::Result(ok, wrap(Ty::Unknown)));
                    }
                    "Err" => {
                        let err = wrap(self.expr_ty(*arg, locals));
                        return Some(Ty::Result(wrap(Ty::Unknown), err));
                    }
                    _ => {}
                }
            }
        }
        let heuristic = self.ctor_type(function);
        let Some(ctx) = self.ctx else {
            return heuristic.map(Ty::Named);
        };
        let segs: Vec<&str> = raw.split("::").collect();
        // `shadowed_names` only guards a bare callee; a path's first segment
        // is a type or module, never a local.
        let shadowed = segs.len() == 1
            && (ctx.shadowed_names.contains(segs[0]) || ctx.shadowed_names.contains("*"));
        let imported = if shadowed {
            None
        } else {
            ctx.imports.get(segs[0])
        };
        // This file's own declarations.
        if !shadowed && imported.is_none() {
            let key = match segs.as_slice() {
                [n] | ["self", n] => Some((*n).to_string()),
                [owner, n] => {
                    let owner = if *owner == "Self" {
                        self.self_ty?
                    } else {
                        owner
                    };
                    owner
                        .starts_with(char::is_uppercase)
                        .then(|| format!("{owner}::{n}"))
                }
                _ => None,
            };
            if let Some(entry) = key.and_then(|k| ctx.returns.get(&k)) {
                return entry.clone();
            }
        }
        let cands: Vec<String> = match (imported, segs[0]) {
            (Some(targets), _) => targets
                .iter()
                .map(|t| {
                    if segs.len() > 1 {
                        format!("{t}::{}", segs[1..].join("::"))
                    } else {
                        t.clone()
                    }
                })
                .collect(),
            (None, "crate") => vec![raw.clone()],
            (None, "self" | "super") if segs.len() > 1 => PROFILE
                .normalize_import_target
                .and_then(|f| f(&raw, &ctx.module))
                .into_iter()
                .collect(),
            _ => Vec::new(),
        };
        if cands.is_empty() {
            return heuristic.map(Ty::Named);
        }
        Some(self.pending(DeferredSource::Call { candidates: cands }, heuristic))
    }

    /// `T::new(..)`/`T::default()`/`T::with_*`/`T::from*` -> `T`.
    fn ctor_type(&self, function: Node<'_>) -> Option<String> {
        if function.kind() != "scoped_identifier" {
            return None;
        }
        let ctor = node_text(function.child_by_field_name("name")?, self.source);
        if !(ctor == "new"
            || ctor == "default"
            || ctor.starts_with("with_")
            || ctor.starts_with("from"))
        {
            return None;
        }
        let path = function.child_by_field_name("path")?;
        let ty = match path.kind() {
            "identifier" => node_text(path, self.source),
            "scoped_identifier" => node_text(path.child_by_field_name("name")?, self.source),
            _ => return None,
        };
        self.usable(ty)
    }

    /// Bind every name in `pat` (matched against a value of type `ty`) over
    /// `scope`. A shape that does not line up with `ty` binds `Unknown`.
    fn bind(&self, pat: Node<'_>, ty: &Ty, scope: Scope, out: &mut Locals) {
        let source = self.source;
        match pat.kind() {
            "identifier" => {
                let name = node_text(pat, source);
                // Uppercase idents are consts / unit variants, not bindings.
                let ty = if name.chars().next().is_some_and(char::is_uppercase) {
                    Ty::Unknown
                } else {
                    ty.clone()
                };
                record_local(out, name, ty, scope);
            }
            "shorthand_field_identifier" => {
                record_local(out, node_text(pat, source), ty.clone(), scope)
            }
            "mut_pattern" | "ref_pattern" | "reference_pattern" => {
                let mut cursor = pat.walk();
                for c in pat.named_children(&mut cursor) {
                    if c.kind() != "mutable_specifier" {
                        self.bind(c, ty, scope, out);
                    }
                }
            }
            "captured_pattern" | "or_pattern" => {
                let mut cursor = pat.walk();
                for c in pat.named_children(&mut cursor) {
                    self.bind(c, ty, scope, out);
                }
            }
            "tuple_pattern" => {
                let mut cursor = pat.walk();
                let elems: Vec<_> = pat.named_children(&mut cursor).collect();
                match ty {
                    Ty::Tuple(tys) if !has_rest(&elems) && tys.len() == elems.len() => {
                        for (e, t) in elems.iter().zip(tys) {
                            self.bind(*e, t, scope, out);
                        }
                    }
                    Ty::Pending(_) if !has_rest(&elems) => {
                        for (i, e) in elems.iter().enumerate() {
                            self.bind(*e, &project(ty.clone(), Step::Tuple(i)), scope, out);
                        }
                    }
                    _ => self.bind_all(pat, scope, out),
                }
            }
            "tuple_struct_pattern" => {
                let type_node = pat.child_by_field_name("type");
                let mut cursor = pat.walk();
                let elems: Vec<_> = pat
                    .named_children(&mut cursor)
                    .filter(|c| Some(*c) != type_node)
                    .collect();
                let step = match type_node.map(|t| node_text(t, source)).as_deref() {
                    Some("Some") => Some(Step::OptionSome),
                    Some("Ok") => Some(Step::ResultOk),
                    Some("Err") => Some(Step::ResultErr),
                    _ => None,
                };
                if let (Some(step), [elem]) = (step, elems.as_slice())
                    && matches!(ty, Ty::Option(_) | Ty::Result(..) | Ty::Pending(_))
                {
                    self.bind(*elem, &project(ty.clone(), step.clone()), scope, out);
                    return;
                }
                match type_node.and_then(|t| self.adt_key(t, ty)) {
                    Some((owner, variant)) if !has_rest(&elems) => {
                        for (i, e) in elems.iter().enumerate() {
                            let field = self.adt_field(&owner, variant.as_deref(), &i.to_string());
                            self.bind(*e, &field, scope, out);
                        }
                    }
                    _ => {
                        for e in elems {
                            self.bind(e, &Ty::Unknown, scope, out);
                        }
                    }
                }
            }
            "struct_pattern" => {
                let key = pat
                    .child_by_field_name("type")
                    .and_then(|t| self.adt_key(t, ty));
                let mut cursor = pat.walk();
                for fp in pat.named_children(&mut cursor) {
                    if fp.kind() != "field_pattern" {
                        continue;
                    }
                    let Some(name_node) = fp.child_by_field_name("name") else {
                        continue;
                    };
                    let name = node_text(name_node, source);
                    let field_ty = key.as_ref().map_or(Ty::Unknown, |(owner, variant)| {
                        self.adt_field(owner, variant.as_deref(), &name)
                    });
                    match fp.child_by_field_name("pattern") {
                        Some(inner) => self.bind(inner, &field_ty, scope, out),
                        None => record_local(out, name, field_ty, scope),
                    }
                }
            }
            "remaining_field_pattern" | "_" => {}
            _ => self.bind_all(pat, scope, out),
        }
    }

    /// Poison every name under `pat` (shapes we do not type).
    fn bind_all(&self, pat: Node<'_>, scope: Scope, out: &mut Locals) {
        let mut cursor = pat.walk();
        for c in pat.named_children(&mut cursor) {
            self.bind(c, &Ty::Unknown, scope, out);
        }
    }

    /// `(type, variant)` a pattern path names when matched against a `Named`
    /// scrutinee: `Type` (struct) or `Type::Variant` (enum), `Self` resolved.
    fn adt_key(&self, path: Node<'_>, scrutinee: &Ty) -> Option<(String, Option<String>)> {
        let Ty::Named(n) = scrutinee else {
            return None;
        };
        let resolve = |s: String| {
            if s == "Self" {
                self.self_ty.map(str::to_string)
            } else {
                Some(s)
            }
        };
        match path.kind() {
            "identifier" | "type_identifier" => {
                (resolve(node_text(path, self.source))? == *n).then(|| (n.clone(), None))
            }
            "scoped_identifier" | "scoped_type_identifier" => {
                let head = path.child_by_field_name("path")?;
                let head = match head.kind() {
                    "identifier" | "type_identifier" => head,
                    "scoped_identifier" | "scoped_type_identifier" => {
                        head.child_by_field_name("name")?
                    }
                    _ => return None,
                };
                let variant = node_text(path.child_by_field_name("name")?, self.source);
                (resolve(node_text(head, self.source))? == *n).then(|| (n.clone(), Some(variant)))
            }
            _ => None,
        }
    }
}

/// Result type of `recv.method()` for the few std methods whose result type
/// follows from the receiver type; everything else is `Unknown`.
fn method_ty(recv: Ty, method: &str) -> Ty {
    match (recv, method) {
        (Ty::Option(t) | Ty::Result(t, _), "unwrap" | "expect") => *t,
        (Ty::Named(n), "unwrap" | "expect") => Ty::Named(n),
        (Ty::Result(t, _), "ok") => Ty::Option(t),
        (Ty::Result(_, e), "err") => Ty::Option(e),
        (t @ (Ty::Option(_) | Ty::Result(..)), "as_ref" | "as_mut" | "clone") => t,
        (t @ Ty::Option(_), "take" | "cloned" | "copied") => t,
        (Ty::Seq(t), "iter" | "into_iter" | "iter_mut" | "drain") => Ty::Iter(t),
        (t @ Ty::Iter(_), "into_iter" | "by_ref") => t,
        (Ty::Iter(t), "next" | "next_back" | "last") => Ty::Option(t),
        (Ty::Seq(t), "pop" | "pop_front" | "pop_back" | "first" | "last") => Ty::Option(t),
        _ => Ty::Unknown,
    }
}

/// Return types of this file's free fns and inherent/trait-impl methods.
fn collect_returns(root: Node<'_>, source: &str) -> Returns {
    fn owner_of(node: Node<'_>, source: &str) -> Option<Option<String>> {
        let list = node.parent()?;
        match list.parent().map(|p| p.kind()) {
            Some("impl_item") => {
                let impl_node = list.parent()?;
                let env = TypeEnv {
                    source,
                    self_ty: None,
                    generics: collect_generic_names(impl_node, source),
                    adts: &Adts::new(),
                    ctx: None,
                };
                Some(Some(env.type_name(impl_node.child_by_field_name("type")?)?))
            }
            // Trait default methods: `Self` is unknown.
            Some("trait_item") => None,
            _ => Some(None),
        }
    }
    fn walk(node: Node<'_>, source: &str, out: &mut Returns) {
        if node.kind() == "function_item" {
            if node.child_by_field_name("body").is_some()
                && let Some(owner) = owner_of(node, source)
                && let Some(name) = node.child_by_field_name("name")
            {
                let empty = Adts::new();
                let env = TypeEnv {
                    source,
                    self_ty: owner.as_deref(),
                    generics: collect_generic_names(node, source),
                    adts: &empty,
                    ctx: None,
                };
                let mut ty = node
                    .child_by_field_name("return_type")
                    .map_or(Ty::Unknown, |t| env.ty(t));
                let mut cursor = node.walk();
                let is_async = node.children(&mut cursor).any(|c| {
                    c.kind() == "function_modifiers" && node_text(c, source).contains("async")
                });
                if is_async {
                    ty = Ty::Future(Box::new(ty));
                }
                let name = node_text(name, source);
                let key = owner.map_or(name.clone(), |o| format!("{o}::{name}"));
                match out.get(&key) {
                    Some(prev) if *prev != Some(ty.clone()) => {
                        out.insert(key, None);
                    }
                    _ => {
                        out.insert(key, Some(ty));
                    }
                }
            }
            return;
        }
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            walk(child, source, out);
        }
    }
    let mut out = Returns::new();
    walk(root, source, &mut out);
    out
}

thread_local! {
    /// One Rust parser per thread, reused across `resolve_deferred` calls.
    static DECLARATION_PARSER: RefCell<Option<Parser>> = const { RefCell::new(None) };
}

fn parse_declaration(src: &str) -> Option<tree_sitter::Tree> {
    DECLARATION_PARSER.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            let mut parser = Parser::new();
            parser
                .set_language(&tree_sitter_rust::LANGUAGE.into())
                .ok()?;
            *slot = Some(parser);
        }
        slot.as_mut()?.parse(src, None)
    })
}

/// `LanguageProfile::deferred_receiver`: finish a `RustDeferred` marker from
/// the declarations it names. Every matching declaration must agree on a
/// type that is declared in the repo; a declaration that isn't a repo type
/// (a `Result`, a future never awaited, a generic) leaves the receiver
/// untracked, and only "no declaration found" keeps the marker's fallback.
fn resolve_deferred(
    column: &str,
    index: &dyn DeclarationIndex,
) -> anyhow::Result<Option<Option<String>>> {
    let Some(deferred) = RustDeferred::decode(column) else {
        return Ok(None);
    };
    let declarations = match &deferred.source {
        DeferredSource::Call { candidates } => {
            let mut all = Vec::new();
            for candidate in candidates {
                all.extend(index.declarations(DeclarationQuery::Callable(candidate))?);
            }
            all
        }
        DeferredSource::Method {
            receiver_type,
            method,
        } => index.declarations(DeclarationQuery::Method(&format!(
            "{receiver_type}::{method}"
        )))?,
        DeferredSource::Field { owner, .. } => index.declarations(DeclarationQuery::Type(owner))?,
    };
    if declarations.is_empty() {
        return Ok(Some(deferred.fallback));
    }
    let mut found: Option<String> = None;
    for declaration in &declarations {
        let Some(signature) = &declaration.signature else {
            return Ok(Some(deferred.fallback));
        };
        let Some(ty) = declared_receiver_type(&deferred, &declaration.qualname, signature) else {
            return Ok(Some(None));
        };
        match &found {
            Some(prev) if *prev != ty => return Ok(Some(None)),
            _ => found = Some(ty),
        }
    }
    Ok(Some(match found {
        Some(ty) if index.is_repo_type(&ty)? => Some(ty),
        _ => None,
    }))
}

/// The receiver type `deferred` reaches through one declaration's indexed
/// signature: a callable's return type or a struct/enum's field type, then
/// the marker's steps.
fn declared_receiver_type(
    deferred: &RustDeferred,
    qualname: &str,
    signature: &str,
) -> Option<String> {
    let name = qualname.rsplit("::").next()?;
    let mut ty = match &deferred.source {
        DeferredSource::Field { field, .. } => {
            // `struct Name<G> { .. }` / `enum Name<G> { .. }` (`adt_signature`).
            let src = if signature.ends_with('}') {
                signature.to_string()
            } else {
                format!("{signature};")
            };
            let tree = parse_declaration(&src)?;
            let adts = collect_adts(tree.root_node(), &src);
            let (variant, field) = match field.split_once("::") {
                Some((v, f)) => (Some(v), f),
                None => (None, field.as_str()),
            };
            let key = variant.map_or_else(|| name.to_string(), |v| format!("{name}::{v}"));
            adts.get(&key)?
                .as_ref()?
                .iter()
                .find(|(n, _)| n == field)?
                .1
                .clone()
        }
        DeferredSource::Call { .. } | DeferredSource::Method { .. } => {
            let (is_async, signature) = split_callable_signature(signature);
            let src = format!("fn __f{signature} {{}}");
            let tree = parse_declaration(&src)?;
            let func = tree.root_node().named_child(0)?;
            if func.kind() != "function_item" {
                return None;
            }
            let owner = qualname
                .rsplit("::")
                .nth(1)
                .filter(|o| o.starts_with(char::is_uppercase));
            let env = TypeEnv {
                source: &src,
                self_ty: owner,
                // The signature keeps the fn's and its impl's `<..>`.
                generics: collect_generic_names(func, &src),
                adts: &Adts::new(),
                ctx: None,
            };
            let ret = env.ty(func.child_by_field_name("return_type")?);
            if is_async {
                Ty::Future(Box::new(ret))
            } else {
                ret
            }
        }
    };
    for step in &deferred.steps {
        ty = project(ty, step.clone());
    }
    match ty {
        Ty::Named(n) => Some(n),
        _ => None,
    }
}

/// `(is_async, rest)` of a callable's indexed signature, past any leading
/// `#[test]`-style attribute lines (`extract_signature`).
fn split_callable_signature(signature: &str) -> (bool, &str) {
    let mut rest = signature;
    while rest.starts_with("#[") {
        rest = rest.split_once('\n').map_or("", |(_, tail)| tail);
    }
    match rest.strip_prefix("async ") {
        Some(tail) => (true, tail),
        None => (false, rest),
    }
}

/// `struct Name<G> { a: T }` / `struct Name(A, B)` / `enum Name { V(A) }`:
/// the declaration's field types, kept in the symbol's signature so other
/// files' extractors can defer to it (`project_deferred`).
fn adt_signature(node: Node<'_>, source: &str) -> Option<String> {
    let flat = |n: Node<'_>| {
        node_text(n, source)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    };
    let fields = |body: Node<'_>| -> String {
        let mut cursor = body.walk();
        match body.kind() {
            "field_declaration_list" => {
                let parts: Vec<String> = body
                    .named_children(&mut cursor)
                    .filter(|f| f.kind() == "field_declaration")
                    .filter_map(|f| {
                        Some(format!(
                            "{}: {}",
                            flat(f.child_by_field_name("name")?),
                            flat(f.child_by_field_name("type")?)
                        ))
                    })
                    .collect();
                format!("{{ {} }}", parts.join(", "))
            }
            "ordered_field_declaration_list" => {
                let parts: Vec<String> = body
                    .children_by_field_name("type", &mut cursor)
                    .map(flat)
                    .collect();
                format!("({})", parts.join(", "))
            }
            _ => String::new(),
        }
    };
    let name = flat(node.child_by_field_name("name")?);
    let generics = node
        .child_by_field_name("type_parameters")
        .map(flat)
        .unwrap_or_default();
    let body = node.child_by_field_name("body");
    if node.kind() == "struct_item" {
        let body = body.map(fields).unwrap_or_default();
        let sep = if body.starts_with('{') { " " } else { "" };
        return Some(format!("struct {name}{generics}{sep}{body}"));
    }
    let mut cursor = body?.walk();
    let variants: Vec<String> = body?
        .named_children(&mut cursor)
        .filter(|v| v.kind() == "enum_variant")
        .filter_map(|v| {
            let vn = flat(v.child_by_field_name("name")?);
            let vb = v
                .child_by_field_name("body")
                .map(fields)
                .unwrap_or_default();
            let sep = if vb.starts_with('{') { " " } else { "" };
            Some(format!("{vn}{sep}{vb}"))
        })
        .collect();
    Some(format!(
        "enum {name}{generics} {{ {} }}",
        variants.join(", ")
    ))
}

/// Struct and enum-variant field types declared in this file.
fn collect_adts(root: Node<'_>, source: &str) -> Adts {
    fn fields_of(env: &TypeEnv<'_>, body: Option<Node<'_>>) -> Vec<(String, Ty)> {
        let Some(body) = body else {
            return Vec::new();
        };
        let mut cursor = body.walk();
        match body.kind() {
            "field_declaration_list" => body
                .named_children(&mut cursor)
                .filter(|f| f.kind() == "field_declaration")
                .filter_map(|f| {
                    let name = node_text(f.child_by_field_name("name")?, env.source);
                    Some((name, env.ty(f.child_by_field_name("type")?)))
                })
                .collect(),
            "ordered_field_declaration_list" => body
                .children_by_field_name("type", &mut cursor)
                .enumerate()
                .map(|(i, t)| (i.to_string(), env.ty(t)))
                .collect(),
            _ => Vec::new(),
        }
    }
    fn walk(node: Node<'_>, source: &str, empty: &Adts, out: &mut Adts) {
        let kind = node.kind();
        if matches!(kind, "struct_item" | "enum_item")
            && let Some(name) = node.child_by_field_name("name")
        {
            let name = node_text(name, source);
            let env = TypeEnv {
                source,
                self_ty: Some(&name),
                generics: collect_generic_names(node, source),
                adts: empty,
                ctx: None,
            };
            let mut entries = Vec::new();
            if kind == "struct_item" {
                entries.push((
                    name.clone(),
                    fields_of(&env, node.child_by_field_name("body")),
                ));
            } else if let Some(list) = node.child_by_field_name("body") {
                // Marks the enum as declared here (see `adt_field`).
                entries.push((name.clone(), Vec::new()));
                let mut cursor = list.walk();
                for v in list.named_children(&mut cursor) {
                    if let Some(vn) = v.child_by_field_name("name") {
                        let key = format!("{name}::{}", node_text(vn, source));
                        entries.push((key, fields_of(&env, v.child_by_field_name("body"))));
                    }
                }
            }
            for (key, fields) in entries {
                let dup = out.contains_key(&key);
                out.insert(key, if dup { None } else { Some(fields) });
            }
        }
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            walk(child, source, empty, out);
        }
    }
    let empty = Adts::new();
    let mut out = Adts::new();
    walk(root, source, &empty, &mut out);
    out
}

/// Generic parameter names of `node` and its enclosing impl/trait items.
fn collect_generic_names(node: Node<'_>, source: &str) -> HashSet<String> {
    let mut out = HashSet::new();
    let mut current = Some(node);
    while let Some(n) = current {
        if let Some(params) = n.child_by_field_name("type_parameters") {
            let mut cursor = params.walk();
            for p in params.named_children(&mut cursor) {
                let id = match p.kind() {
                    "type_identifier" => Some(p),
                    "constrained_type_parameter" => p.child_by_field_name("left"),
                    "type_parameter" | "optional_type_parameter" => p.child_by_field_name("name"),
                    _ => None,
                };
                if let Some(id) = id {
                    out.insert(node_text(id, source));
                }
            }
        }
        current = n.parent();
    }
    out
}

/// End of the block an `if let`/`while let` condition guards: the
/// consequence/body of the nearest `if`/`while` whose condition holds it.
fn let_scope_end(cond: Node<'_>) -> Option<usize> {
    let mut current = cond.parent();
    while let Some(n) = current {
        if matches!(n.kind(), "if_expression" | "while_expression") {
            let block = n
                .child_by_field_name("consequence")
                .or_else(|| n.child_by_field_name("body"))?;
            return Some(block.end_byte());
        }
        current = n.parent();
    }
    None
}

/// Locally-knowable receiver types for one function, scoped: typed params
/// (`x: T`, `&T`, `&mut T<'_>`), `let` bindings (annotated, `T::new(..)`,
/// `T { .. }`, or a known-typed scrutinee), `if let`/`while let`/`match`/
/// `for` pattern bindings (typed when the scrutinee type is known, else
/// poisoned) and closure params. Nested `fn` items are collected on their own.
fn collect_local_types(node: Node<'_>, env: &TypeEnv<'_>, out: &mut Locals) {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "function_item" => continue,
            "parameter" => {
                if let (Some(pat), Some(owner)) = (
                    child.child_by_field_name("pattern"),
                    child.parent().and_then(|p| p.parent()),
                ) {
                    let ty = child.child_by_field_name("type").map(|t| env.ty(t));
                    let scope = (owner.start_byte(), owner.end_byte());
                    env.bind(pat, &ty.unwrap_or(Ty::Unknown), scope, out);
                }
            }
            "let_declaration" => {
                if let (Some(pat), Some(block)) =
                    (child.child_by_field_name("pattern"), child.parent())
                {
                    let mut ty = child
                        .child_by_field_name("type")
                        .map_or(Ty::Unknown, |t| env.ty(t));
                    if ty == Ty::Unknown
                        && let Some(v) = child.child_by_field_name("value")
                    {
                        ty = env.expr_ty(v, out);
                    }
                    env.bind(pat, &ty, (child.end_byte(), block.end_byte()), out);
                }
            }
            "let_condition" => {
                if let (Some(pat), Some(value)) = (
                    child.child_by_field_name("pattern"),
                    child.child_by_field_name("value"),
                ) {
                    // No enclosing `if`/`while`: poison the names for the rest
                    // of the fn instead of leaking an outer binding's type.
                    let (ty, end) = match let_scope_end(child) {
                        Some(end) => (env.expr_ty(value, out), end),
                        None => (Ty::Unknown, usize::MAX),
                    };
                    env.bind(pat, &ty, (value.end_byte(), end), out);
                }
            }
            "for_expression" => {
                if let (Some(pat), Some(value), Some(body)) = (
                    child.child_by_field_name("pattern"),
                    child.child_by_field_name("value"),
                    child.child_by_field_name("body"),
                ) {
                    let ty = project(env.expr_ty(value, out), Step::Elem);
                    env.bind(pat, &ty, (value.end_byte(), body.end_byte()), out);
                }
            }
            "match_arm" => {
                let scrutinee = child
                    .parent()
                    .and_then(|block| block.parent())
                    .and_then(|m| m.child_by_field_name("value"));
                if let Some(mp) = child.child_by_field_name("pattern") {
                    let ty = scrutinee.map_or(Ty::Unknown, |v| env.expr_ty(v, out));
                    let guard = mp.child_by_field_name("condition");
                    let mut c = mp.walk();
                    let scope = (child.start_byte(), child.end_byte());
                    for p in mp.named_children(&mut c).filter(|p| Some(*p) != guard) {
                        env.bind(p, &ty, scope, out);
                    }
                }
            }
            "closure_expression" => {
                if let Some(params) = child.child_by_field_name("parameters") {
                    let scope = (child.start_byte(), child.end_byte());
                    let mut c = params.walk();
                    // Typed `|e: T|` params are handled as `parameter` below.
                    for p in params
                        .named_children(&mut c)
                        .filter(|p| p.kind() != "parameter")
                    {
                        env.bind(p, &Ty::Unknown, scope, out);
                    }
                }
            }
            _ => {}
        }
        collect_local_types(child, env, out);
    }
}

/// Detect std::env::var("KEY"), env::var("KEY"), env::var_os("KEY"), dotenvy::var("KEY") → CONFIG_READ
fn config_read_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function_node = node.child_by_field_name("function")?;
    let raw_fn = node_text(function_node, source);

    // Match env::var, std::env::var, env::var_os, std::env::var_os, dotenvy::var
    let is_env_var = raw_fn == "env::var"
        || raw_fn == "std::env::var"
        || raw_fn == "env::var_os"
        || raw_fn == "std::env::var_os"
        || raw_fn == "dotenvy::var";

    if !is_env_var {
        return None;
    }

    let args = call_arguments(node);
    let key_node = args.first()?;
    let key = extract_string_literal(*key_node, source)?;
    let env_uri = config::normalize_env_var_name(&key)?;
    let framework = if raw_fn.starts_with("dotenvy") {
        "dotenvy"
    } else {
        "rust-std"
    };
    let detail = config::build_config_read_detail("env", &env_uri, &key, framework);
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

fn channel_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function_node = node.child_by_field_name("function")?;
    let target = call_target_parts(function_node, source)?;
    let receiver = target.receiver.as_deref()?;
    if !channel::is_bus_receiver(receiver) {
        return None;
    }
    let kind = if channel::is_publish_method(&target.name) {
        channel::CHANNEL_PUBLISH_KIND
    } else if channel::is_subscribe_method(&target.name) {
        channel::CHANNEL_SUBSCRIBE_KIND
    } else {
        return None;
    };
    let args = call_arguments(node);
    let raw_topic = args
        .first()
        .and_then(|arg| extract_string_literal(*arg, source))
        .or_else(|| args.first().map(|arg| node_text(*arg, source)))?;
    let normalized = channel::normalize_channel_name(&raw_topic)?;
    let detail = if kind == channel::CHANNEL_PUBLISH_KIND {
        channel::build_publish_detail(&normalized, &raw_topic, "rust-bus")
    } else {
        channel::build_subscribe_detail(&normalized, &raw_topic, "rust-bus")
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

#[derive(Clone)]
struct AttributeInfo<'a> {
    full_name: String,
    short_name: String,
    args: Option<Node<'a>>,
    node: Node<'a>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AttributeTokenKind {
    Identifier,
    StringLiteral,
}

struct AttributeToken {
    start: i64,
    text: String,
    kind: AttributeTokenKind,
}

struct CallTarget {
    receiver: Option<String>,
    name: String,
    full: String,
}

#[derive(Clone)]
struct GrpcService {
    package: Option<String>,
    service: String,
}

struct ActixRouteReceiver {
    resource_path: Option<String>,
    scope_prefix: Option<String>,
}

fn route_edges_from_attribute_items(
    attributes: &[Node<'_>],
    _ctx: &Context,
    source: &str,
    handler: &str,
) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    for attr in attribute_infos(attributes, source) {
        let name = attr.short_name.to_ascii_lowercase();
        let framework = framework_from_attribute(&attr.full_name);
        if let Some(method) = http::normalize_method(&name) {
            let raw_path = attr
                .args
                .and_then(|args| {
                    let tokens = attribute_tokens(args, source);
                    tokens
                        .iter()
                        .find(|token| token.kind == AttributeTokenKind::StringLiteral)
                        .map(|token| token.text.clone())
                        .or_else(|| extract_string_from_text(&node_text(args, source)))
                })
                .unwrap_or_else(|| "/".to_string());
            if let Some(edge) =
                build_route_edge(handler, &method, &raw_path, framework, attr.node, source)
            {
                edges.push(edge);
            }
            continue;
        }
        if name == "route" {
            let Some(args) = attr.args else {
                continue;
            };
            let tokens = attribute_tokens(args, source);
            let raw_path = tokens
                .iter()
                .find(|token| token.kind == AttributeTokenKind::StringLiteral)
                .map(|token| token.text.clone())
                .or_else(|| extract_string_from_text(&node_text(args, source)));
            let Some(raw_path) = raw_path else {
                continue;
            };
            let mut methods = Vec::new();
            for (idx, token) in tokens.iter().enumerate() {
                if token.kind == AttributeTokenKind::Identifier
                    && (token.text == "method" || token.text == "methods")
                    && let Some(next) = tokens
                        .iter()
                        .skip(idx + 1)
                        .find(|next| next.kind == AttributeTokenKind::StringLiteral)
                    && let Some(method) = http::normalize_method(&next.text)
                {
                    methods.push(method);
                }
            }
            if methods.is_empty() {
                methods.push(http::HTTP_ANY.to_string());
            }
            for method in methods {
                if let Some(edge) =
                    build_route_edge(handler, &method, &raw_path, framework, attr.node, source)
                {
                    edges.push(edge);
                }
            }
        }
    }
    edges
}

fn attribute_infos<'a>(attributes: &[Node<'a>], source: &str) -> Vec<AttributeInfo<'a>> {
    let mut out = Vec::new();
    for child in attributes {
        let Some(attr_node) = find_child_of_kind(*child, "attribute") else {
            continue;
        };
        if let Some(info) = attribute_info(attr_node, source) {
            out.push(info);
        }
    }
    out
}

fn attribute_info<'a>(node: Node<'a>, source: &str) -> Option<AttributeInfo<'a>> {
    let path_node = attribute_path_node(node)?;
    let full_name = node_text(path_node, source);
    if full_name.is_empty() {
        return None;
    }
    let short_name = full_name
        .split("::")
        .last()
        .unwrap_or(full_name.as_str())
        .to_string();
    let args = node.child_by_field_name("arguments");
    Some(AttributeInfo {
        full_name,
        short_name,
        args,
        node,
    })
}

fn attribute_path_node(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "identifier"
            | "scoped_identifier"
            | "self"
            | "super"
            | "crate"
            | "metavariable"
            | "reserved_identifier" => {
                return Some(child);
            }
            _ => {}
        }
    }
    None
}

fn attribute_tokens(node: Node<'_>, source: &str) -> Vec<AttributeToken> {
    let mut tokens = Vec::new();
    collect_attribute_tokens(node, source, &mut tokens);
    tokens.sort_by_key(|token| token.start);
    tokens
}

fn collect_attribute_tokens(node: Node<'_>, source: &str, tokens: &mut Vec<AttributeToken>) {
    match node.kind() {
        "identifier" | "scoped_identifier" => {
            let text = node_text(node, source);
            if !text.is_empty() {
                tokens.push(AttributeToken {
                    start: node.start_byte() as i64,
                    text,
                    kind: AttributeTokenKind::Identifier,
                });
            }
        }
        "string_literal" | "raw_string_literal" => {
            if let Some(text) = extract_string_literal(node, source) {
                tokens.push(AttributeToken {
                    start: node.start_byte() as i64,
                    text,
                    kind: AttributeTokenKind::StringLiteral,
                });
            }
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_attribute_tokens(child, source, tokens);
    }
}

fn framework_from_attribute(full_name: &str) -> &'static str {
    let lower = full_name.to_ascii_lowercase();
    if lower.contains("actix") {
        "actix"
    } else if lower.contains("rocket") {
        "rocket"
    } else if lower.contains("axum") {
        "axum"
    } else {
        "rust"
    }
}

fn build_route_edge(
    handler: &str,
    method: &str,
    raw_path: &str,
    framework: &str,
    node: Node<'_>,
    source: &str,
) -> Option<EdgeInput> {
    let mut path = raw_path.trim().to_string();
    if !path.starts_with('/') {
        path = format!("/{path}");
    }
    let normalized = http::normalize_path(&path)?;
    let detail = http::build_route_detail(method, &normalized, &path, framework);
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    Some(EdgeInput {
        kind: http::HTTP_ROUTE_KIND.to_string(),
        source_qualname: Some(handler.to_string()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

fn http_route_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    edges.extend(route_call_edges(node, ctx, source));
    edges
}

fn route_call_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    let Some(function) = node.child_by_field_name("function") else {
        return edges;
    };
    let Some(target) = call_target_parts(function, source) else {
        return edges;
    };
    if target.name != "route" {
        return edges;
    }
    let args = call_arguments(node);
    let receiver_paths = actix_receiver_paths(function, source);
    let (raw_path, route_arg, used_receiver_path) = if let Some(raw_path) = args
        .first()
        .and_then(|arg| extract_string_literal(*arg, source))
    {
        (Some(raw_path), args.get(1).copied(), false)
    } else {
        let raw_path = receiver_paths
            .resource_path
            .clone()
            .or(receiver_paths.scope_prefix.clone());
        (raw_path.clone(), args.first().copied(), raw_path.is_some())
    };
    let Some(mut raw_path) = raw_path else {
        return edges;
    };
    let Some(route_arg) = route_arg else {
        return edges;
    };
    let Some((method, handler, framework)) =
        method_and_handler_from_route_arg(route_arg, ctx, source)
    else {
        return edges;
    };
    let handler = handler.unwrap_or_else(|| ctx.current_scope.clone());
    if let Some(prefix) = receiver_paths.scope_prefix.as_deref()
        && !used_receiver_path
    {
        raw_path = http::join_paths(prefix, &raw_path);
    }
    if let Some(edge) = build_route_edge(&handler, &method, &raw_path, framework, node, source) {
        edges.push(edge);
    }
    edges
}

fn method_and_handler_from_route_arg(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
) -> Option<(String, Option<String>, &'static str)> {
    if let Some((method, handler)) = axum_method_and_handler_from_route_arg(node, ctx, source) {
        return Some((method, handler, "axum"));
    }
    if let Some((method, handler)) = actix_method_and_handler_from_route_arg(node, ctx, source) {
        return Some((method, handler, "actix"));
    }
    None
}

fn axum_method_and_handler_from_route_arg(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
) -> Option<(String, Option<String>)> {
    if node.kind() != "call_expression" {
        return None;
    }
    let function = node.child_by_field_name("function")?;
    let target = call_target_parts(function, source)?;
    let method = http::normalize_method(&target.name)?;
    let args = call_arguments(node);
    let handler = args
        .first()
        .and_then(|arg| handler_name_from_expr(*arg, ctx, source));
    Some((method, handler))
}

fn actix_method_and_handler_from_route_arg(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
) -> Option<(String, Option<String>)> {
    if node.kind() != "call_expression" {
        return None;
    }
    let function = node.child_by_field_name("function")?;
    let target = call_target_parts(function, source)?;
    if target.name != "to" && target.name != "to_async" && target.name != "to_sync" {
        return None;
    }
    let receiver = function.child_by_field_name("value")?;
    let method = actix_method_from_builder(receiver, source)?;
    let args = call_arguments(node);
    let handler = args
        .first()
        .and_then(|arg| handler_name_from_expr(*arg, ctx, source));
    Some((method, handler))
}

fn actix_method_from_builder(node: Node<'_>, source: &str) -> Option<String> {
    let mut current = node;
    loop {
        if current.kind() != "call_expression" {
            return None;
        }
        let function = current.child_by_field_name("function")?;
        let target = call_target_parts(function, source)?;
        if let Some(method) = http::normalize_method(&target.name) {
            return Some(method);
        }
        let receiver = function.child_by_field_name("value")?;
        if receiver.kind() != "call_expression" {
            return None;
        }
        current = receiver;
    }
}

fn actix_receiver_paths(function: Node<'_>, source: &str) -> ActixRouteReceiver {
    let mut out = ActixRouteReceiver {
        resource_path: None,
        scope_prefix: None,
    };
    if function.kind() != "field_expression" {
        return out;
    }
    let Some(receiver) = function.child_by_field_name("value") else {
        return out;
    };
    let Some((name, path)) = actix_call_name_and_path(receiver, source) else {
        return out;
    };
    match name.as_str() {
        "resource" => out.resource_path = Some(path),
        "scope" => out.scope_prefix = Some(path),
        _ => {}
    }
    out
}

fn actix_call_name_and_path(node: Node<'_>, source: &str) -> Option<(String, String)> {
    if node.kind() != "call_expression" {
        return None;
    }
    let function = node.child_by_field_name("function")?;
    let target = call_target_parts(function, source)?;
    let name = target.name.to_ascii_lowercase();
    if name != "resource" && name != "scope" {
        return None;
    }
    let args = call_arguments(node);
    let path = args
        .first()
        .and_then(|arg| extract_string_literal(*arg, source))?;
    Some((name, path))
}

fn handler_name_from_expr(node: Node<'_>, ctx: &Context, source: &str) -> Option<String> {
    let raw = node_text(node, source);
    if raw.is_empty() {
        return None;
    }
    let collapsed = collapse_call_target_whitespace(&raw);
    resolve_call_target(&collapsed, ctx).or(Some(raw))
}

fn http_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function = node.child_by_field_name("function")?;
    let target = call_target_parts(function, source)?;
    let client = http_client_label(target.receiver.as_deref(), &target.full)?;
    let args = call_arguments(node);
    let (method, raw_path) = if target.name == "request" {
        let method = args
            .first()
            .and_then(|arg| extract_method_from_expr(*arg, source))?;
        let raw_path = args
            .get(1)
            .and_then(|arg| extract_string_literal(*arg, source))?;
        (method, raw_path)
    } else if let Some(method) = http::normalize_method(&target.name) {
        let raw_path = args
            .first()
            .and_then(|arg| extract_string_literal(*arg, source))?;
        (method, raw_path)
    } else {
        return None;
    };
    let normalized = http::normalize_path(&raw_path)?;
    let detail = http::build_call_detail(&method, &normalized, &raw_path, client);
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

fn grpc_impl_edge(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    rpc_name: &str,
    qualname: &str,
) -> Option<EdgeInput> {
    let service = ctx.grpc_service.as_ref()?;
    let (raw_path, normalized) = tonic_rpc_path(service, rpc_name)?;
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    let detail = json!({
        "framework": "tonic",
        "role": "server",
        "service": service.service.as_str(),
        "rpc": rpc_name,
        "package": service.package.as_deref(),
        "raw": raw_path,
    })
    .to_string();
    Some(EdgeInput {
        kind: proto::RPC_IMPL_KIND.to_string(),
        source_qualname: Some(qualname.to_string()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

fn grpc_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function = node.child_by_field_name("function")?;
    let target = call_target_parts(function, source)?;
    if is_grpc_client_constructor(&target.name) {
        return None;
    }
    let service = grpc_service_for_receiver(target.receiver.as_deref(), ctx)?;
    let (raw_path, normalized) = tonic_rpc_path(&service, &target.name)?;
    let detail = json!({
        "framework": "tonic",
        "role": "client",
        "service": service.service.as_str(),
        "rpc": target.name,
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

fn tonic_rpc_path(service: &GrpcService, rpc_name: &str) -> Option<(String, String)> {
    let compact = rpc_name.replace('_', "");
    let service_path = grpc_service_path(service);
    let raw = format!("/{service_path}/{rpc_name}");
    let (_raw, normalized) =
        proto::normalize_rpc_path(service.package.as_deref(), &service.service, &compact)?;
    Some((raw, normalized))
}

fn grpc_service_path(service: &GrpcService) -> String {
    match service.package.as_deref() {
        Some(package) if !package.is_empty() => format!("{package}.{}", service.service),
        _ => service.service.clone(),
    }
}

fn is_grpc_client_constructor(name: &str) -> bool {
    matches!(name, "connect" | "new" | "with_interceptor")
}

fn grpc_service_for_receiver(receiver: Option<&str>, ctx: &Context) -> Option<GrpcService> {
    let receiver = receiver?.trim();
    if receiver.is_empty() {
        return None;
    }
    if let Some(service) = ctx.grpc_clients.get(receiver) {
        return Some(service.clone());
    }
    if let Some(last) = receiver.rsplit("::").next() {
        let last = last.rsplit('.').next().unwrap_or(last);
        if let Some(service) = ctx.grpc_clients.get(last) {
            return Some(service.clone());
        }
    }
    grpc_service_from_client_path(receiver, &ctx.imports)
}

fn grpc_service_from_trait(trait_name: &str) -> Option<GrpcService> {
    let parts: Vec<&str> = trait_name
        .split("::")
        .filter(|part| !part.is_empty())
        .collect();
    let mut server_idx = None;
    for (idx, part) in parts.iter().enumerate() {
        if part.ends_with("_server") {
            server_idx = Some(idx);
            break;
        }
    }
    let idx = server_idx?;
    let service = parts.get(idx + 1)?.trim();
    if service.is_empty() {
        return None;
    }
    let package = grpc_package_from_parts(&parts[..idx]);
    Some(GrpcService {
        package,
        service: service.to_string(),
    })
}

fn grpc_service_from_client_path(
    path: &str,
    imports: &HashMap<String, Vec<String>>,
) -> Option<GrpcService> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Expand the leading segment through this scope's `use` bindings so
    // `SyncServiceClient::new` recovers the package from
    // `use crate::proto::sync::v1::sync_service_client::SyncServiceClient`.
    let expanded;
    let (first, rest) = trimmed.split_once("::").unwrap_or((trimmed, ""));
    let trimmed = match imports.get(first).and_then(|targets| targets.first()) {
        Some(target) if rest.is_empty() => target.as_str(),
        Some(target) => {
            expanded = format!("{target}::{rest}");
            expanded.as_str()
        }
        None => trimmed,
    };
    let parts: Vec<&str> = trimmed
        .split("::")
        .filter(|part| !part.is_empty())
        .collect();
    let type_name = parts.last()?.trim();
    let type_name = type_name.split('<').next().unwrap_or(type_name).trim();
    let service = type_name.strip_suffix("Client")?;
    if service.is_empty() {
        return None;
    }
    let package = parts
        .iter()
        .position(|part| part.ends_with("_client"))
        .and_then(|idx| grpc_package_from_parts(&parts[..idx]));
    Some(GrpcService {
        package,
        service: service.to_string(),
    })
}

fn grpc_package_from_parts(parts: &[&str]) -> Option<String> {
    let filtered: Vec<&str> = parts
        .iter()
        .copied()
        .filter(|part| !matches!(*part, "crate" | "self" | "super"))
        .collect();
    if filtered.is_empty() {
        None
    } else {
        Some(filtered.join("."))
    }
}

fn collect_grpc_clients(
    node: Node<'_>,
    source: &str,
    imports: &HashMap<String, Vec<String>>,
) -> HashMap<String, GrpcService> {
    let mut clients = HashMap::new();
    collect_grpc_clients_inner(node, source, imports, &mut clients);
    clients
}

fn collect_grpc_clients_inner(
    node: Node<'_>,
    source: &str,
    imports: &HashMap<String, Vec<String>>,
    clients: &mut HashMap<String, GrpcService>,
) {
    if node.kind() == "function_item" || node.kind() == "impl_item" {
        return;
    }
    if (node.kind() == "let_declaration" || node.kind() == "let_statement")
        && let Some((name, service)) = grpc_client_from_let(node, source, imports)
    {
        clients.insert(name, service);
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_grpc_clients_inner(child, source, imports, clients);
    }
}

fn grpc_client_from_let(
    node: Node<'_>,
    source: &str,
    imports: &HashMap<String, Vec<String>>,
) -> Option<(String, GrpcService)> {
    let pattern = node
        .child_by_field_name("pattern")
        .or_else(|| node.child_by_field_name("name"))?;
    let name = pattern_identifier(pattern, source)?;
    let service = node
        .child_by_field_name("value")
        .or_else(|| node.child_by_field_name("initializer"))
        .and_then(|value| grpc_client_from_expr(value, source, imports))
        .or_else(|| grpc_client_from_expr(node, source, imports))?;
    Some((name, service))
}

fn grpc_client_from_expr(
    node: Node<'_>,
    source: &str,
    imports: &HashMap<String, Vec<String>>,
) -> Option<GrpcService> {
    if node.kind() == "call_expression" {
        let function = node.child_by_field_name("function")?;
        let target = call_target_parts(function, source)?;
        if is_grpc_client_constructor(&target.name)
            && let Some(receiver) = target.receiver.as_deref()
        {
            return grpc_service_from_client_path(receiver, imports);
        }
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(service) = grpc_client_from_expr(child, source, imports) {
            return Some(service);
        }
    }
    None
}

fn pattern_identifier(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "identifier" {
        let name = node_text(node, source);
        if !name.is_empty() {
            return Some(name);
        }
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(name) = pattern_identifier(child, source) {
            return Some(name);
        }
    }
    None
}

fn call_arguments(node: Node<'_>) -> Vec<Node<'_>> {
    let mut args = Vec::new();
    let Some(list) = node.child_by_field_name("arguments") else {
        return args;
    };
    let mut cursor = list.walk();
    for child in list.named_children(&mut cursor) {
        if child.kind() == "attribute_item" {
            continue;
        }
        args.push(child);
    }
    args
}

fn call_target_parts(node: Node<'_>, source: &str) -> Option<CallTarget> {
    let full = node_text(node, source);
    if full.is_empty() {
        return None;
    }
    match node.kind() {
        "field_expression" => {
            let receiver = node
                .child_by_field_name("value")
                .map(|value| node_text(value, source))
                .filter(|value| !value.is_empty());
            let name = node
                .child_by_field_name("field")
                .map(|field| node_text(field, source))
                .unwrap_or_else(|| full.clone());
            Some(CallTarget {
                receiver,
                name,
                full,
            })
        }
        "scoped_identifier" => {
            let name = node
                .child_by_field_name("name")
                .map(|name| node_text(name, source))
                .unwrap_or_else(|| split_last_segment(&full).1);
            let receiver = node
                .child_by_field_name("path")
                .map(|path| node_text(path, source))
                .filter(|value| !value.is_empty());
            Some(CallTarget {
                receiver,
                name,
                full,
            })
        }
        "identifier" => Some(CallTarget {
            receiver: None,
            name: full.clone(),
            full,
        }),
        // A turbofished call (`x.foo::<T>()`, `foo::<T>()`) parses as a
        // generic_function whose `function` field holds the real callee. Resolve
        // receiver/name from that inner node so the turbofish suffix never leaks
        // into the bare method name; keep `full` as the complete callee text.
        "generic_function" => {
            let inner = node.child_by_field_name("function")?;
            let inner_parts = call_target_parts(inner, source)?;
            Some(CallTarget {
                receiver: inner_parts.receiver,
                name: inner_parts.name,
                full,
            })
        }
        _ => {
            let (receiver, name) = split_last_segment(&full);
            Some(CallTarget {
                receiver,
                name,
                full,
            })
        }
    }
}

fn split_last_segment(raw: &str) -> (Option<String>, String) {
    if let Some((left, right)) = raw.rsplit_once("::") {
        return (Some(left.to_string()), right.to_string());
    }
    if let Some((left, right)) = raw.rsplit_once('.') {
        return (Some(left.to_string()), right.to_string());
    }
    (None, raw.to_string())
}

fn extract_string_literal(node: Node<'_>, source: &str) -> Option<String> {
    match node.kind() {
        "string_literal" | "raw_string_literal" => {
            let raw = node_text(node, source);
            unquote_rust_string(&raw)
        }
        _ => None,
    }
}

fn extract_string_from_text(raw: &str) -> Option<String> {
    let chars = raw.char_indices();
    let mut quote = None;
    let mut start = 0;
    for (idx, ch) in chars {
        if ch == '"' || ch == '\'' {
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

fn unquote_rust_string(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut value = trimmed;
    if let Some(rest) = value.strip_prefix('b') {
        value = rest;
    }
    if let Some(rest) = value.strip_prefix('r') {
        let mut hash_count = 0;
        let mut idx = 0;
        for ch in rest.chars() {
            if ch == '#' {
                hash_count += 1;
                idx += ch.len_utf8();
                continue;
            }
            if ch == '"' {
                idx += ch.len_utf8();
                break;
            }
            return None;
        }
        let content = &rest[idx..];
        let suffix = format!("\"{}", "#".repeat(hash_count));
        if content.ends_with(&suffix) {
            let end = content.len() - suffix.len();
            return Some(content[..end].to_string());
        }
        return None;
    }
    if value.starts_with('"') && value.ends_with('"') && value.len() >= 2 {
        return Some(value[1..value.len() - 1].to_string());
    }
    None
}

fn extract_method_from_expr(node: Node<'_>, source: &str) -> Option<String> {
    if let Some(raw) = extract_string_literal(node, source) {
        return http::normalize_method(&raw);
    }
    let raw = node_text(node, source);
    if raw.is_empty() {
        return None;
    }
    let last = raw.split("::").last().unwrap_or(raw.as_str());
    let last = last.split('.').next_back().unwrap_or(last);
    http::normalize_method(last)
}

fn http_client_label(receiver: Option<&str>, full: &str) -> Option<&'static str> {
    let full_lower = full.to_ascii_lowercase();
    let receiver_lower = receiver.unwrap_or("").to_ascii_lowercase();
    if full_lower.contains("reqwest") || receiver_lower.contains("reqwest") {
        return Some("reqwest");
    }
    if full_lower.contains("ureq") || receiver_lower.contains("ureq") {
        return Some("ureq");
    }
    if receiver_lower.ends_with("client") || receiver_lower.contains("client") {
        return Some("http_client");
    }
    None
}

/// Import-tier candidates for a bare call `raw`: the targets this scope's
/// `use` bound it to, unless the current function shadows it
/// (`collect_shadowed_names`).
fn import_qualified_candidates(raw: &str, ctx: &Context) -> Vec<String> {
    if raw.is_empty() || raw.contains("::") || raw.contains('.') {
        return Vec::new();
    }
    if ctx.shadowed_names.contains(raw) || ctx.shadowed_names.contains("*") {
        return Vec::new();
    }
    ctx.imports.get(raw).cloned().unwrap_or_default()
}

/// Names a bare call in this function must not get an import candidate for:
/// any name that occurs in the body (excluding nested `fn` items) other than
/// as a call's callee — a pattern binding, value use or in-function `use`.
/// A glob `use` inside the body adds `*`, which suppresses every name.
/// Over-suppression only falls back to the name tiers; it never mis-binds.
fn collect_shadowed_names(node: Node<'_>, source: &str, out: &mut HashSet<String>) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "function_item" {
            continue; // its own, independent analysis
        }
        if child.kind() == "use_wildcard" {
            out.insert("*".to_string());
        } else if matches!(child.kind(), "identifier" | "shorthand_field_identifier")
            && !is_call_callee(child)
        {
            out.insert(node_text(child, source));
        }
        collect_shadowed_names(child, source, out);
    }
}

/// Whether `node` is exactly the `function` field of its parent
/// `call_expression` — a bare `helper` in `helper()`, but not in
/// `helper(x)`'s arguments, `let helper = ...`, `Some(helper)`, etc.
fn is_call_callee(node: Node<'_>) -> bool {
    node.parent().is_some_and(|parent| {
        parent.kind() == "call_expression" && parent.child_by_field_name("function") == Some(node)
    })
}

/// Apply `PROFILE.normalize_import_target` to a raw import/call target,
/// keeping it unchanged when it needed no rewrite (already absolute, e.g.
/// `crate::...`, or a plain name).
fn normalized_import_target(raw: String, module: &str) -> String {
    PROFILE
        .normalize_import_target
        .and_then(|rewrite| rewrite(&raw, module))
        .unwrap_or(raw)
}

/// Resolve a call's target text (already whitespace-collapsed — see
/// `collapse_call_target_whitespace`) to an absolute qualname, or `None`
/// when it isn't a shape this extractor can qualify.
fn resolve_call_target(raw: &str, ctx: &Context) -> Option<String> {
    if raw.is_empty() || !is_simple_call_target(raw) {
        return None;
    }
    if let Some(container) = ctx.container_stack.last() {
        if let Some(rest) = raw
            .strip_prefix("self::")
            .or_else(|| raw.strip_prefix("Self::"))
        {
            if rest.is_empty() {
                return None;
            }
            return Some(format!("{container}::{rest}"));
        }
        if let Some(rest) = raw.strip_prefix("self.") {
            if rest.is_empty() || rest.contains('.') {
                return None;
            }
            return Some(format!("{container}::{rest}"));
        }
    }
    // Module-relative path prefixes, independent of any enclosing
    // impl/trait: an impl introduces no module scope of its own, so
    // `super::` (and top-level `self::`, i.e. outside the container branch
    // above, which already claimed `self::`/`Self::` as impl-relative) are
    // always relative to `ctx.module`, the *lexical* module the call site
    // sits in.
    if let Some(rewrite) = PROFILE.normalize_import_target
        && let Some(target) = rewrite(raw, &ctx.module)
    {
        return Some(target);
    }
    if raw.contains("::") {
        return Some(raw.to_string());
    }
    if raw.contains('.') {
        return None;
    }
    let base = ctx
        .container_stack
        .last()
        .cloned()
        .unwrap_or_else(|| ctx.module.clone());
    Some(format!("{base}::{raw}"))
}

fn is_simple_call_target(raw: &str) -> bool {
    raw.chars()
        .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == ':' || ch == '.')
}

fn body_node(node: Node<'_>) -> Option<Node<'_>> {
    if let Some(body) = node.child_by_field_name("body") {
        return Some(body);
    }
    find_child_of_kind(node, "declaration_list")
}

fn find_child_of_kind<'a>(node: Node<'a>, kind: &str) -> Option<Node<'a>> {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .find(|&child| child.kind() == kind)
}

fn extract_name(node: Node<'_>, source: &str) -> Option<String> {
    if let Some(name_node) = node.child_by_field_name("name") {
        let name = node_text(name_node, source);
        if !name.is_empty() {
            return Some(name);
        }
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            "identifier" | "type_identifier" | "scoped_identifier" => {
                let name = node_text(child, source);
                if !name.is_empty() {
                    return Some(name);
                }
            }
            _ => {}
        }
    }
    None
}

fn normalize_type_path(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    let mut cleaned = String::new();
    let mut depth = 0;
    for ch in trimmed.chars() {
        match ch {
            '<' => {
                depth += 1;
            }
            '>' => {
                if depth > 0 {
                    depth -= 1;
                }
            }
            _ if depth > 0 => {}
            _ => cleaned.push(ch),
        }
    }
    let cleaned = cleaned.replace(' ', "");
    cleaned.trim_start_matches('&').trim().to_string()
}

fn qualify_type_name(module: &str, type_name: &str) -> String {
    if type_name.starts_with("self::") {
        let suffix = type_name.trim_start_matches("self::");
        return format!("{module}::{suffix}");
    }
    if type_name.starts_with("crate::")
        || type_name.starts_with("super::")
        || type_name.starts_with("::")
        || type_name.contains("::")
    {
        return type_name.to_string();
    }
    format!("{module}::{type_name}")
}

/// Whether `node` (a `function_item`) carries a leading `pub`/`pub(...)`
/// visibility modifier — tree-sitter-rust exposes it as a direct
/// `visibility_modifier` child regardless of which `pub(...)` form is
/// used. Absence means module-private: only callers in the same file can
/// see it (see `db::resolver::VisibilityRule::Recorded`, issue #75).
/// `pub(crate)`/`pub(super)`/etc. are all treated as public here — lidx
/// doesn't model crate boundaries, so the distinction between them doesn't
/// change which calls should be allowed to bind.
fn has_pub_visibility(node: Node<'_>) -> bool {
    let mut cursor = node.walk();
    node.children(&mut cursor)
        .any(|c| c.kind() == "visibility_modifier")
}

/// Attribute short names (the identifier after the last `::`, e.g. `test`
/// for both `#[test]` and `#[tokio::test]`) that mark a function as a test.
/// `signature` is the only place a Rust attribute reaches
/// `test_detection::is_test_symbol` -- see `test_attribute_prefix` and
/// issue #67 finding 1.
const TEST_ATTRIBUTE_NAMES: &[&str] = &["test", "rstest", "test_case"];

/// Renders any test-marking attribute in `attributes` (see
/// `TEST_ATTRIBUTE_NAMES`) back out as `#[full_name]\n...` so
/// `extract_signature` can prefix it onto the signature, giving
/// `test_detection::is_test_symbol`'s signature check something to see.
/// Attribute arguments (e.g. `#[rstest(case(1, 2))]`) are dropped -- only
/// presence matters here.
fn test_attribute_prefix(attributes: &[Node<'_>], source: &str) -> Option<String> {
    let mut prefix = String::new();
    for info in attribute_infos(attributes, source) {
        if TEST_ATTRIBUTE_NAMES.contains(&info.short_name.as_str()) {
            prefix.push_str(&format!("#[{}]\n", info.full_name));
        }
    }
    if prefix.is_empty() {
        None
    } else {
        Some(prefix)
    }
}

/// Primitives and ubiquitous std types: never a repo symbol, so no `USES` edge.
fn is_std_type_name(name: &str) -> bool {
    matches!(
        name,
        "Self"
            | "str"
            | "bool"
            | "char"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "usize"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "isize"
            | "f32"
            | "f64"
            | "String"
            | "Vec"
            | "Option"
            | "Result"
            | "Box"
            | "Rc"
            | "Arc"
            | "HashMap"
            | "HashSet"
            | "Some"
            | "None"
            | "Ok"
            | "Err"
    )
}

/// Emits `USES` edges for type references (field/param/return types, struct
/// literals) and functions passed as values (`get_or_init(Config::from_env)`),
/// none of which are calls. Sourced from the enclosing module: `dead_symbols`
/// only needs to know the target is referenced somewhere.
fn collect_uses(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    seen: &mut HashSet<(String, String)>,
) {
    let mut ctx = ctx.clone();
    if node.kind() == "mod_item"
        && let Some(name) = extract_name(node, source)
    {
        ctx.module = format!("{}::{name}", ctx.module);
    }
    if !matches!(node.kind(), "use_declaration" | "attribute_item") {
        let parent = node.parent();
        let is_value_arg =
            node.kind() == "identifier" && parent.is_some_and(|p| p.kind() == "arguments");
        let is_ref = matches!(
            node.kind(),
            "type_identifier" | "scoped_type_identifier" | "scoped_identifier"
        ) || is_value_arg;
        let is_inner_path = parent
            .is_some_and(|p| matches!(p.kind(), "scoped_type_identifier" | "scoped_identifier"));
        let is_definition_name = parent.is_some_and(|p| {
            p.child_by_field_name("name") == Some(node)
                || (p.kind() == "impl_item" && p.child_by_field_name("type") == Some(node))
        });
        if is_ref && !is_inner_path && !is_definition_name && !is_call_callee(node) {
            let raw = node_text(node, source);
            if !is_std_type_name(&raw)
                && let Some(target) = resolve_call_target(&raw, &ctx)
                && seen.insert((ctx.module.clone(), target.clone()))
            {
                output.edges.push(EdgeInput {
                    kind: "USES".to_string(),
                    source_qualname: Some(ctx.module.clone()),
                    target_qualname: Some(target),
                    import_candidates: import_qualified_candidates(&raw, &ctx),
                    ..Default::default()
                });
            }
        }
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            collect_uses(child, &ctx, source, output, seen);
        }
    }
}

fn extract_signature(node: Node<'_>, source: &str, attributes: &[Node<'_>]) -> Option<String> {
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(n, source));
    let return_type = node
        .child_by_field_name("return_type")
        .map(|n| node_text(n, source));
    // The fn's and its impl's generic parameters, so `project_deferred`-style
    // readers can tell `-> Item` (a parameter) from a type named `Item`.
    let generics = signature_generics(node, source);
    let base = match (params, return_type) {
        (Some(p), Some(r)) => Some(format!("{generics}{p} -> {r}")),
        (Some(p), None) => Some(format!("{generics}{p}")),
        _ => None,
    };
    // Calling an `async fn` yields a future; `resolve_deferred` reads this.
    let mut cursor = node.walk();
    let is_async = node
        .children(&mut cursor)
        .any(|c| c.kind() == "function_modifiers" && node_text(c, source).contains("async"));
    let base = base.map(|b| if is_async { format!("async {b}") } else { b });
    match (test_attribute_prefix(attributes, source), base) {
        (Some(prefix), Some(base)) => Some(format!("{prefix}{base}")),
        (Some(prefix), None) => Some(prefix),
        (None, base) => base,
    }
}

/// `<Item, T: Bound>`: the generic parameters of `node` and of the impl/trait
/// enclosing it (impl's first), or `""` when there are none.
fn signature_generics(node: Node<'_>, source: &str) -> String {
    let mut owners = vec![node];
    let mut current = node.parent();
    while let Some(n) = current {
        if matches!(n.kind(), "impl_item" | "trait_item") {
            owners.push(n);
        }
        current = n.parent();
    }
    let parts: Vec<String> = owners
        .iter()
        .rev()
        .filter_map(|n| n.child_by_field_name("type_parameters"))
        .filter_map(|params| {
            let text = node_text(params, source);
            let inner = text.strip_prefix('<')?.strip_suffix('>')?;
            Some(inner.split_whitespace().collect::<Vec<_>>().join(" "))
        })
        .filter(|inner| !inner.is_empty())
        .collect();
    if parts.is_empty() {
        String::new()
    } else {
        format!("<{}>", parts.join(", "))
    }
}

/// This scope's *own* `use`/`pub use` bindings, keyed by the name each
/// introduces — direct children of `scope` (a `source_file` or a `mod`'s
/// `declaration_list`) only, not recursing into a nested `mod`/`impl`/
/// `trait`/function body: each of those is a separate scope with its own
/// bindings (a Rust module doesn't inherit its parent's `use`s), collected
/// separately when that scope is entered (see `extract`, `handle_mod`).
/// `self::`/`super::` targets are normalized against `module` (this
/// scope's own qualname) via `PROFILE.normalize_import_target`, shared
/// with `resolve_call_target`'s call-path rewrite.
fn collect_use_bindings(
    scope: Node<'_>,
    source: &str,
    module: &str,
) -> HashMap<String, Vec<String>> {
    let mut out: HashMap<String, Vec<String>> = HashMap::new();
    let mut cursor = scope.walk();
    for child in scope.named_children(&mut cursor) {
        if !matches!(child.kind(), "use_declaration" | "use_item") {
            continue;
        }
        let text = node_text(child, source);
        for (bound, raw_target) in parse_use_bindings(&text) {
            let target = normalized_import_target(raw_target, module);
            let entry = out.entry(bound).or_default();
            if !entry.contains(&target) {
                entry.push(target);
            }
        }
    }
    out
}

/// Strip a `use` declaration's leading visibility (`pub`, `pub(crate)`,
/// `pub(super)`, `pub(self)`, `pub(in some::path)`) and the `use` keyword
/// itself. `None` when `text` (already newline-collapsed and
/// semicolon-trimmed) isn't a `use` declaration at all.
fn strip_use_prefix(text: &str) -> Option<&str> {
    let rest = text.trim_start();
    let rest = match rest.strip_prefix("pub") {
        Some(after_pub) => {
            let after_pub = after_pub.trim_start();
            match after_pub.strip_prefix('(') {
                Some(paren_rest) => {
                    let close = paren_rest.find(')')?;
                    paren_rest[close + 1..].trim_start()
                }
                None => after_pub,
            }
        }
        None => rest,
    };
    rest.strip_prefix("use ")
}

/// Parse a `use` declaration's text into `(bound_name, raw_target)` pairs —
/// the name it introduces into scope, and the target as written (still
/// `self::`/`super::`-relative when it is; callers normalize that via
/// `normalized_import_target`). The one `use`-tree parser, shared by
/// `handle_use` (IMPORTS edges) and `collect_use_bindings` (the CALLS
/// import tier's candidates).
fn parse_use_bindings(text: &str) -> Vec<(String, String)> {
    let cleaned = text.replace('\n', " ");
    let cleaned = cleaned.trim().trim_end_matches(';');
    let Some(rest) = strip_use_prefix(cleaned) else {
        return Vec::new();
    };
    let rest = rest.trim();
    if rest.is_empty() {
        return Vec::new();
    }
    expand_use_bindings(rest)
}

/// Expands a `use` tree fragment (post visibility/`use ` prefix stripping)
/// into `(bound_name, raw_target)` pairs. A glob (`foo::*`) binds no single
/// name — its bound name comes back as `"*"`, which can never collide with
/// a real Rust identifier, so it's harmless as a map key nobody looks up.
fn expand_use_bindings(input: &str) -> Vec<(String, String)> {
    let input = input.trim();
    if input.is_empty() {
        return Vec::new();
    }
    if let Some((before, inner)) = split_outer_braces(input) {
        let base = before.trim().trim_end_matches("::").trim().to_string();
        let items = split_top_level(inner.as_str(), ',');
        let mut results = Vec::new();
        for item in items {
            let item = item.trim();
            if item.is_empty() {
                continue;
            }
            let combined = if base.is_empty() {
                item.to_string()
            } else {
                format!("{base}::{item}")
            };
            results.extend(expand_use_bindings(&combined));
        }
        return results;
    }

    let (main, alias) = match input.split_once(" as ") {
        Some((left, right)) => (left.trim(), Some(right.trim())),
        None => (input, None),
    };
    let main = main.trim_end_matches("::self");
    if main.is_empty() {
        return Vec::new();
    }
    let bound = match alias {
        Some(alias) if !alias.is_empty() => alias.to_string(),
        _ => main.rsplit("::").next().unwrap_or(main).to_string(),
    };
    vec![(bound, main.to_string())]
}

fn split_outer_braces(input: &str) -> Option<(String, String)> {
    let mut depth = 0;
    let mut start = None;
    for (idx, ch) in input.char_indices() {
        match ch {
            '{' => {
                if depth == 0 {
                    start = Some(idx);
                }
                depth += 1;
            }
            '}' if depth > 0 => {
                depth -= 1;
                if depth == 0 {
                    let start_idx = start?;
                    let before = input[..start_idx].to_string();
                    let inner = input[start_idx + 1..idx].to_string();
                    return Some((before, inner));
                }
            }
            _ => {}
        }
    }
    None
}

fn split_top_level(input: &str, delimiter: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (idx, ch) in input.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' if depth > 0 => {
                depth -= 1;
            }
            _ if ch == delimiter && depth == 0 => {
                parts.push(input[start..idx].to_string());
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    if start <= input.len() {
        parts.push(input[start..].to_string());
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::RustExtractor;
    use crate::indexer::extract::LanguageExtractor;
    use crate::indexer::extract::{DeferredSource, ReceiverType, RustDeferred, Step};
    use crate::indexer::http;
    use crate::indexer::proto;

    const USES_SRC: &str = r#"
pub struct Foo;
pub trait T { fn req(&self); }
impl T for Foo { fn req(&self) {} }
impl Foo { pub fn inherent(&self) {} }
pub fn a(x: Foo, y: Foo, n: u32, s: String, v: Vec<Foo>) -> Option<Foo> { None }
#[test_case(1)]
fn tc() {}
"#;

    /// Receiver types of every `x.go()` call in `src`, in source order.
    fn go_receivers(src: &str) -> Vec<String> {
        let file = RustExtractor::new().unwrap().extract(src, "crate").unwrap();
        file.edges
            .iter()
            .filter(|e| e.kind == "CALLS" && e.target_qualname.as_deref() == Some("go"))
            .map(|e| match &e.receiver_type {
                ReceiverType::Known(t) => t.clone(),
                ReceiverType::Deferred(m) => m.clone(),
                _ => "-".to_string(),
            })
            .collect()
    }

    fn call(steps: Vec<Step>) -> RustDeferred {
        RustDeferred {
            source: DeferredSource::Call {
                candidates: vec!["crate::a::f".into()],
            },
            steps,
            fallback: None,
        }
    }

    #[test]
    fn test_attribute_before_async_is_still_read_as_async() {
        let src = "pub struct Engine;\n#[test]\npub async fn t() -> Engine { Engine }";
        let file = RustExtractor::new().unwrap().extract(src, "crate").unwrap();
        let sig = file
            .symbols
            .iter()
            .find(|s| s.name == "t")
            .and_then(|s| s.signature.clone())
            .unwrap();
        assert!(sig.starts_with("#[test]\nasync "), "{sig}");
        assert_eq!(
            super::declared_receiver_type(&call(vec![]), "crate::a::t", &sig),
            None
        );
        assert_eq!(
            super::declared_receiver_type(&call(vec![Step::Await]), "crate::a::t", &sig),
            Some("Engine".to_string())
        );
    }

    #[test]
    fn generic_parameters_of_fn_and_impl_are_not_receiver_types() {
        let src = "pub struct Item;
pub struct W<T>(T);
impl<Item> W<Item> { pub fn get(self) -> Item { todo!() } pub fn plain(self) -> Item2 { todo!() } }
pub fn make<Item>() -> Item { todo!() }
pub fn concrete() -> Item { todo!() }";
        let file = RustExtractor::new().unwrap().extract(src, "crate").unwrap();
        let sig = |name: &str| {
            file.symbols
                .iter()
                .find(|s| s.name == name)
                .and_then(|s| s.signature.clone())
                .unwrap()
        };
        let receiver = |name: &str, qualname: &str| {
            super::declared_receiver_type(&call(vec![]), qualname, &sig(name))
        };
        assert_eq!(receiver("get", "crate::W::get"), None);
        assert_eq!(receiver("make", "crate::make"), None);
        assert_eq!(receiver("concrete", "crate::concrete"), Some("Item".into()));
    }

    #[test]
    fn same_block_shadowing_types_each_call_by_position() {
        let src = "pub struct A; pub struct B;
fn f() {
    let e = A::new();
    e.go();
    let e = B::new();
    e.go();
    {
        let e = A::new();
        e.go();
    }
    e.go();
    let x = 1;
    if let Some(e) = x { e.go(); }
    e.go();
}";
        assert_eq!(go_receivers(src), ["A", "B", "A", "B", "-", "B"]);
    }

    #[test]
    fn uses_edges_are_deduped_and_skip_primitives() {
        let file = RustExtractor::new()
            .unwrap()
            .extract(USES_SRC, "crate")
            .unwrap();
        let uses: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == "USES")
            .filter_map(|e| e.target_qualname.clone())
            .collect();
        assert_eq!(
            uses.iter().filter(|t| *t == "crate::Foo").count(),
            1,
            "{uses:?}"
        );
        assert!(
            !uses
                .iter()
                .any(|t| t.ends_with("::u32") || t.ends_with("::String")),
            "{uses:?}"
        );
    }

    #[test]
    fn trait_impl_methods_are_marked_by_edge_not_signature() {
        let file = RustExtractor::new()
            .unwrap()
            .extract(USES_SRC, "crate")
            .unwrap();
        for sym in &file.symbols {
            let sig = sym.signature.clone().unwrap_or_default();
            assert!(!sig.contains("trait_method"), "{}: {sig}", sym.qualname);
        }
        assert!(
            !file.edges.iter().any(|e| e.kind == "TRAIT_IMPL_METHOD"),
            "pseudo-edge must not exist"
        );
    }

    #[test]
    fn trait_impl_methods_get_method_level_implements_edge() {
        let file = RustExtractor::new()
            .unwrap()
            .extract(USES_SRC, "crate")
            .unwrap();
        let edges: Vec<_> = file
            .edges
            .iter()
            .filter(|e| {
                e.kind == "IMPLEMENTS" && e.source_qualname.as_deref() == Some("crate::Foo::req")
            })
            .collect();
        assert_eq!(edges.len(), 1, "{edges:?}");
        assert_eq!(edges[0].target_qualname.as_deref(), Some("crate::T::req"));
        assert!(!file.edges.iter().any(|e| e.kind == "IMPLEMENTS"
            && e.source_qualname.as_deref() == Some("crate::Foo::inherent")));
    }

    #[test]
    fn test_case_attribute_marks_test() {
        let file = RustExtractor::new()
            .unwrap()
            .extract(USES_SRC, "crate")
            .unwrap();
        let tc = file.symbols.iter().find(|s| s.name == "tc").unwrap();
        assert!(
            tc.signature
                .clone()
                .unwrap_or_default()
                .contains("test_case")
        );
    }

    #[test]
    fn extracts_route_attribute_and_reqwest_call() {
        let source = r#"
#[get("/api/users/{id}")]
async fn handler() {}

fn main() {
    let _ = reqwest::get("/api/users/123");
}
"#;
        let mut extractor = RustExtractor::new().unwrap();
        let file = extractor.extract(source, "crate").unwrap();
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

    #[test]
    fn extracts_actix_route_builders() {
        let source = r#"
use actix_web::{web, App};

async fn handler() {}

fn main() {
    App::new().route("/api/users/{id}", web::get().to(handler));
    web::resource("/api/items/{id}").route(web::post().to(handler));
    web::scope("/api").route("/v1/users/{id}", web::get().to(handler));
}
"#;
        let mut extractor = RustExtractor::new().unwrap();
        let file = extractor.extract(source, "crate").unwrap();
        let routes = file
            .edges
            .iter()
            .filter(|edge| edge.kind == http::HTTP_ROUTE_KIND)
            .collect::<Vec<_>>();
        assert!(
            routes
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/api/users/{}"))
        );
        assert!(
            routes
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/api/items/{}"))
        );
        assert!(
            routes
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/api/v1/users/{}"))
        );
    }

    #[test]
    fn tonic_client_package_recovered_from_use_import() {
        let source = r#"
use crate::proto::sync::v1::sync_service_client::SyncServiceClient;
use crate::proto::sync::v1::health_client;

async fn run(ch: Channel) {
    let mut client = SyncServiceClient::new(ch);
    client.sync(req).await.unwrap();
    let mut h = health_client::HealthClient::new(ch2);
    h.check(req).await.unwrap();
}
"#;
        let mut extractor = RustExtractor::new().unwrap();
        let file = extractor.extract(source, "crate").unwrap();
        let targets: Vec<_> = file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_CALL_KIND)
            .filter_map(|edge| edge.target_qualname.as_deref())
            .collect();
        assert!(
            targets.contains(&"/proto.sync.v1.syncservice/sync"),
            "{targets:?}"
        );
        assert!(
            targets.contains(&"/proto.sync.v1.health/check"),
            "{targets:?}"
        );
    }

    #[test]
    fn extracts_tonic_grpc_impl_and_call() {
        let source = r#"
impl helloworld::greeter_server::Greeter for MyGreeter {
    async fn say_hello(&self) {}
}

async fn run() {
    let mut client = helloworld::greeter_client::GreeterClient::connect("http://localhost")
        .await
        .unwrap();
    client.say_hello().await.unwrap();
}
"#;
        let mut extractor = RustExtractor::new().unwrap();
        let file = extractor.extract(source, "crate").unwrap();
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
        assert!(impls
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("/helloworld.greeter/sayhello")));
        assert!(calls
            .iter()
            .any(|edge| edge.target_qualname.as_deref() == Some("/helloworld.greeter/sayhello")));
    }
}
