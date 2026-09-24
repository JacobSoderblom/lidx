use crate::db::resolver::{ImportMissPolicy, LanguageProfile, VisibilityRule};
use crate::indexer::channel;
use crate::indexer::config;
use crate::indexer::extract::{EdgeInput, ExtractedFile, SymbolInput};
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
use std::path::Path;
use std::rc::Rc;
use tree_sitter::{Node, Parser};

/// Rust's resolution profile. `use` targets are absolute after
/// `normalize_import_target`, so suffix matching is off.
pub(crate) const PROFILE: LanguageProfile = LanguageProfile {
    separators: &["::"],
    normalize_import_target: Some(normalize_import_target),
    import_miss: ImportMissPolicy::FallThrough,
    import_suffix_matching: false,
    visibility: VisibilityRule::Recorded,
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
}

pub struct RustExtractor {
    parser: Parser,
}

impl RustExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_rust::LANGUAGE;
        parser.set_language(&language.into())?;
        Ok(Self { parser })
    }
}

impl crate::indexer::extract::LanguageExtractor for RustExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String {
        module_name_from_rel_path(rel_path)
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
        };
        walk_node(root, &ctx, source, &mut output);
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
            handle_function(node, ctx, source, output);
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
        signature: None,
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

fn handle_function(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
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
    let signature = extract_signature(node, source);
    if !has_pub_visibility(node) {
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
    if let Some(edge) = grpc_impl_edge(node, ctx, source, &name, &qualname) {
        output.edges.push(edge);
    }
    if let Some(body) = node.child_by_field_name("body") {
        let mut next_ctx = ctx.clone();
        next_ctx.current_scope = qualname;
        let mut grpc_clients = ctx.grpc_clients.clone();
        grpc_clients.extend(collect_grpc_clients(body, source));
        next_ctx.grpc_clients = grpc_clients;

        // Recomputed per function, never inherited.
        let mut shadowed = HashSet::new();
        collect_shadowed_names(node, source, &mut shadowed);
        next_ctx.shadowed_names = Rc::new(shadowed);

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
    handle_function(node, ctx, source, output);
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
    let signature = extract_signature(node, source);
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
    let target = resolve_call_target(&collapsed, ctx);
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
        ..Default::default()
    });
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
    grpc_service_from_client_path(receiver)
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

fn grpc_service_from_client_path(path: &str) -> Option<GrpcService> {
    let trimmed = path.trim();
    if trimmed.is_empty() {
        return None;
    }
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

fn collect_grpc_clients(node: Node<'_>, source: &str) -> HashMap<String, GrpcService> {
    let mut clients = HashMap::new();
    collect_grpc_clients_inner(node, source, &mut clients);
    clients
}

fn collect_grpc_clients_inner(
    node: Node<'_>,
    source: &str,
    clients: &mut HashMap<String, GrpcService>,
) {
    if node.kind() == "function_item" || node.kind() == "impl_item" {
        return;
    }
    if (node.kind() == "let_declaration" || node.kind() == "let_statement")
        && let Some((name, service)) = grpc_client_from_let(node, source)
    {
        clients.insert(name, service);
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_grpc_clients_inner(child, source, clients);
    }
}

fn grpc_client_from_let(node: Node<'_>, source: &str) -> Option<(String, GrpcService)> {
    let pattern = node
        .child_by_field_name("pattern")
        .or_else(|| node.child_by_field_name("name"))?;
    let name = pattern_identifier(pattern, source)?;
    let service = node
        .child_by_field_name("value")
        .or_else(|| node.child_by_field_name("initializer"))
        .and_then(|value| grpc_client_from_expr(value, source))
        .or_else(|| grpc_client_from_expr(node, source))?;
    Some((name, service))
}

fn grpc_client_from_expr(node: Node<'_>, source: &str) -> Option<GrpcService> {
    if node.kind() == "call_expression" {
        let function = node.child_by_field_name("function")?;
        let target = call_target_parts(function, source)?;
        if is_grpc_client_constructor(&target.name)
            && let Some(receiver) = target.receiver.as_deref()
        {
            return grpc_service_from_client_path(receiver);
        }
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(service) = grpc_client_from_expr(child, source) {
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

fn extract_signature(node: Node<'_>, source: &str) -> Option<String> {
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(n, source));
    let return_type = node
        .child_by_field_name("return_type")
        .map(|n| node_text(n, source));
    match (params, return_type) {
        (Some(p), Some(r)) => Some(format!("{p} -> {r}")),
        (Some(p), None) => Some(p),
        _ => None,
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
    use crate::indexer::http;
    use crate::indexer::proto;

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
