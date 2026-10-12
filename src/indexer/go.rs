use crate::db::resolver::{
    GO_IMPORTED_PACKAGE, GO_SAME_PACKAGE, GO_UNKNOWN_QUALIFIER, LanguageProfile, VisibilityRule,
};
use crate::edge_views;
use crate::indexer::channel;
use crate::indexer::config;
use crate::indexer::extract::{
    CallShape, EdgeInput, ExtractedFile, GoDeferred, GoValue, ReceiverType, SymbolInput,
};
use crate::indexer::go_repo::{GoRepo, ImportTarget};
use crate::indexer::go_types::LocalTypes;
use crate::indexer::http;
use crate::indexer::proto;
use crate::indexer::string_consts::{LocalBinding, StringConsts};
use crate::indexer::tree_helpers::{
    collapse_call_target_whitespace, collect_identifier_names, module_symbol_fallback,
    module_symbol_with_span, node_text, span,
};
use crate::util;
use anyhow::Result;
use serde_json::json;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tree_sitter::{Node, Parser};

/// Go's resolution profile: the shared default, plus capitalization-based
/// visibility — no extractor recording needed (`VisibilityRule::
/// GoCapitalization` derives it from a candidate's trailing name and
/// compares package directories directly), so this extractor never touches
/// `ExtractedFile::private_qualnames`.
pub(crate) const PROFILE: LanguageProfile = LanguageProfile {
    visibility: VisibilityRule::GoCapitalization,
    two_segment: false,
    call_refused_kinds: &["class", "interface", "type", "variable"],
    deferred_candidate: Some(crate::db::resolver::go_deferred_candidate),
    ..LanguageProfile::DEFAULT
};

/// A local variable declaration tagged with the byte range of the block that
/// directly contains it, for scope-aware resolution.
#[derive(Clone)]
struct ScopedVarType {
    name: String,
    type_name: String,
    /// Byte offset of the start of the containing block / func_literal.
    block_start: usize,
    /// Byte offset of the end of the containing block / func_literal.
    block_end: usize,
}

#[derive(Clone)]
struct Context {
    /// Same-file string constants (see `string_consts`), used to resolve
    /// channel topics given as identifiers.
    string_consts: Rc<StringConsts>,
    module: String,
    current_scope: String,
    grpc_servers: HashMap<String, GrpcServerInfo>,
    grpc_clients: HashMap<String, GrpcClientInfo>,
    /// Local variable declarations with their containing-block byte ranges.
    local_var_types: Vec<ScopedVarType>,
    /// Import alias (or default package name) -> import path, file-wide.
    imports: Rc<HashMap<String, String>>,
    /// Names the current function binds itself: receiver, parameters, `:=`,
    /// `var`, `range` and type-switch variables (`collect_bound_names`).
    bound_names: Rc<HashSet<String>>,
    /// Receiver and parameters whose declared type is a plain local type
    /// name, so `s.Handle` can name a method of that type.
    param_types: Rc<HashMap<String, String>>,
    /// The declared or nameable types of the current function's bindings,
    /// closures included (`go_types`).
    types: Rc<LocalTypes>,
}

#[derive(Clone)]
struct GrpcServerInfo {
    service: String,
}

#[derive(Clone)]
struct GrpcClientInfo {
    service: String,
}

pub struct GoExtractor {
    parser: Parser,
    /// The repo's Go modules, scanned on first use in an index run.
    repo: RefCell<Option<(PathBuf, Rc<GoRepo>)>>,
}

impl GoExtractor {
    /// The repo's Go modules, scanned once per index run.
    fn repo(&self, repo_root: &Path) -> Rc<GoRepo> {
        let mut slot = self.repo.borrow_mut();
        match slot.as_ref() {
            Some((root, repo)) if root == repo_root => repo.clone(),
            _ => {
                let repo = Rc::new(GoRepo::scan(repo_root));
                *slot = Some((repo_root.to_path_buf(), repo.clone()));
                repo
            }
        }
    }

    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_go::LANGUAGE;
        parser.set_language(&language.into())?;
        Ok(Self {
            parser,
            repo: RefCell::new(None),
        })
    }
}

impl crate::indexer::extract::LanguageExtractor for GoExtractor {
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
                    .push(module_symbol_fallback(module_name, source, "/", None));
                return Ok(output);
            }
        };
        let root = tree.root_node();

        let module_span = span(root);
        output
            .symbols
            .push(module_symbol_with_span(module_name, module_span, "/", None));

        // First pass: collect gRPC server types
        let grpc_servers = collect_grpc_servers(root, source);

        let ctx = Context {
            string_consts: Rc::new(crate::indexer::string_consts::collect_string_consts(
                crate::indexer::string_consts::ConstLang::Go,
                root,
                source,
            )),
            module: module_name.to_string(),
            current_scope: module_name.to_string(),
            grpc_servers,
            grpc_clients: HashMap::new(),
            local_var_types: Vec::new(),
            imports: Rc::new(collect_import_aliases(root, source)),
            bound_names: Rc::new(HashSet::new()),
            param_types: Rc::new(HashMap::new()),
            types: Rc::new(LocalTypes::default()),
        };
        walk_node(root, &ctx, source, &mut output);
        Ok(output)
    }

    fn begin_run(&mut self) {
        // A go.mod edited since the last run must be seen, as a fresh index would.
        *self.repo.borrow_mut() = None;
    }

    fn resolve_imports(
        &self,
        repo_root: &Path,
        file_rel_path: &str,
        _module_name: &str,
        edges: &mut Vec<EdgeInput>,
    ) {
        let repo = self.repo(repo_root);
        bind_import_candidates(&repo, repo_root, file_rel_path, edges);
        // The import path, the name the file calls the package by (the
        // extractor left it in `detail`) and the package it stands for
        // (`><dir>` inside the repository, else the path): a deferred
        // receiver's type text is read through these.
        for edge in edges.iter_mut() {
            if edge.kind == "IMPORTS"
                && let Some(target) = edge.target_qualname.as_ref()
            {
                let place = match repo
                    .has_modules()
                    .then(|| repo.resolve_import(repo_root, file_rel_path, target))
                {
                    Some(ImportTarget::InRepo(dir)) => format!("{GO_IMPORTED_PACKAGE}{dir}"),
                    _ => target.clone(),
                };
                edge.detail = Some(
                    json!({
                        "import_path": target,
                        "bound_name": edge.detail,
                        "place": place,
                    })
                    .to_string(),
                );
            }
        }
    }
}

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "main".to_string();
    }
    let file = parts.pop().unwrap_or_default();
    let stem = Path::new(&file)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(&file)
        .to_string();
    if !stem.is_empty() {
        parts.push(stem);
    }
    if parts.is_empty() {
        "main".to_string()
    } else {
        parts.join("/")
    }
}

fn collect_grpc_servers(root: Node<'_>, source: &str) -> HashMap<String, GrpcServerInfo> {
    let mut servers = HashMap::new();
    let mut cursor = root.walk();
    for child in root.named_children(&mut cursor) {
        if child.kind() == "type_declaration" {
            collect_grpc_servers_from_type_decl(child, source, &mut servers);
        }
    }
    servers
}

fn collect_grpc_servers_from_type_decl(
    node: Node<'_>,
    source: &str,
    servers: &mut HashMap<String, GrpcServerInfo>,
) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "type_spec" {
            let Some(name_node) = child.child_by_field_name("name") else {
                continue;
            };
            let name = node_text(name_node, source);
            if name.is_empty() {
                continue;
            }

            let Some(type_node) = child.child_by_field_name("type") else {
                continue;
            };

            if type_node.kind() == "struct_type"
                && is_grpc_server_struct(type_node, source)
                && let Some(service) = extract_grpc_service_from_server_name(&name)
            {
                servers.insert(name, GrpcServerInfo { service });
            }
        }
    }
}

fn walk_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if matches!(node.kind(), "identifier" | "selector_expression")
        && let Some(edge) = value_reference_edge(node, ctx, source)
    {
        output.edges.push(edge);
    }
    match node.kind() {
        "source_file" => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                walk_node(child, ctx, source, output);
            }
        }
        "function_declaration" => {
            handle_function(node, ctx, source, output);
        }
        "method_declaration" => {
            handle_method(node, ctx, source, output);
        }
        "type_declaration" => {
            handle_type_declaration(node, ctx, source, output);
        }
        "const_declaration" => {
            handle_const_declaration(node, ctx, source, output);
        }
        "var_declaration" => {
            handle_var_declaration(node, ctx, source, output);
        }
        "import_declaration" => {
            handle_import(node, ctx, source, output);
        }
        // `Map[int](x)` with a single operand parses as a conversion to the
        // generic type `Map[int]`; it is a call of the generic function
        // unless `Map` is a type (`List[int](x)` converts), which a later
        // change makes the resolver refuse to bind.
        "type_conversion_expression" => {
            if let Some(type_node) = node
                .child_by_field_name("type")
                .filter(|t| t.kind() == "generic_type")
            {
                emit_calls_edge(node, type_node, ctx, source, output);
            }
            walk_children(node, ctx, source, output);
        }
        // A closure in a package-level initializer binds its own parameters.
        "func_literal" if ctx.current_scope == ctx.module => {
            let mut next_ctx = ctx.clone();
            enter_function_scope(node, source, &mut next_ctx);
            walk_children(node, &next_ctx, source, output);
        }
        "call_expression" => {
            handle_call(node, ctx, source, output);
            // Every named child can hold more calls: the callee (`a.B().C()`,
            // `func(){..}()`, `f[T](x)`) and the arguments (`f(g(x))`,
            // `t.Run("n", func(t *testing.T){..})`). Closure bodies keep the
            // enclosing declaration as their scope.
            walk_children(node, ctx, source, output);
        }
        _ => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                walk_node(child, ctx, source, output);
            }
        }
    }
}

fn handle_function(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = format!("{}.{}", ctx.module, name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = extract_function_signature(node, source);
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
        source_qualname: Some(ctx.module.clone()),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    let mut next_ctx = ctx.clone();
    next_ctx.current_scope = qualname;
    enter_function_scope(node, source, &mut next_ctx);
    if let Some(body) = node.child_by_field_name("body") {
        // Collect gRPC clients from variable declarations in the function body
        let mut clients = ctx.grpc_clients.clone();
        clients.extend(collect_grpc_clients(body, source));
        next_ctx.grpc_clients = clients;
        // Collect local variable types for config bind resolution (scope-aware)
        let mut var_types = ctx.local_var_types.clone();
        var_types.extend(collect_local_var_types(body, source));
        next_ctx.local_var_types = var_types;
        walk_node(body, &next_ctx, source, output);
    }
}

fn handle_method(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }

    let receiver_type = extract_receiver_type(node, source);
    let qualname = if let Some(ref rtype) = receiver_type {
        format!("{}.{}.{}", ctx.module, rtype, name)
    } else {
        format!("{}.{}", ctx.module, name)
    };

    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = extract_function_signature(node, source);
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

    if let Some(ref rtype) = receiver_type {
        let parent_qualname = format!("{}.{}", ctx.module, rtype);
        output.edges.push(EdgeInput {
            kind: "CONTAINS".to_string(),
            source_qualname: Some(parent_qualname),
            target_qualname: Some(qualname.clone()),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });

        // Check if this is a gRPC server implementation
        if let Some(edge) = grpc_impl_edge(node, ctx, source, &name, &qualname, rtype) {
            output.edges.push(edge);
        }
    }

    let mut next_ctx = ctx.clone();
    next_ctx.current_scope = qualname;
    enter_function_scope(node, source, &mut next_ctx);
    if let Some(body) = node.child_by_field_name("body") {
        // Collect gRPC clients from variable declarations in the method body
        let mut clients = ctx.grpc_clients.clone();
        clients.extend(collect_grpc_clients(body, source));
        next_ctx.grpc_clients = clients;
        // Collect local variable types for config bind resolution
        let mut var_types = ctx.local_var_types.clone();
        var_types.extend(collect_local_var_types(body, source));
        next_ctx.local_var_types = var_types;
        walk_node(body, &next_ctx, source, output);
    }
}

fn handle_type_declaration(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "type_spec" {
            handle_type_spec(child, ctx, source, output);
        }
    }
}

fn handle_type_spec(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }

    let Some(type_node) = node.child_by_field_name("type") else {
        return;
    };

    let signature = (type_node.kind() == "struct_type")
        .then(|| struct_signature(type_node, source))
        .flatten();
    let kind = match type_node.kind() {
        "struct_type" => "class",
        "interface_type" => {
            // Check for interface embedding (extends)
            handle_interface_embedding(node, ctx, source, output, &name);
            "interface"
        }
        _ => "type",
    };

    let qualname = format!("{}.{}", ctx.module, name);
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
        signature,
        docstring: None,
        identity: None,
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

fn handle_const_declaration(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        let name = if child.kind() == "const_spec" {
            extract_name_from_spec(child, source)
        } else {
            None
        };
        if let Some(name) = name {
            let qualname = format!("{}.{}", ctx.module, name);
            let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(child);
            output.symbols.push(SymbolInput {
                kind: "variable".to_string(),
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
                source_qualname: Some(ctx.module.clone()),
                target_qualname: Some(qualname),
                detail: None,
                evidence_snippet: None,
                ..Default::default()
            });
        }
    }
}

fn handle_var_declaration(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    // Only top-level vars are symbols (current_scope == module); a local
    // `var` is walked in the enclosing function's scope.
    if ctx.current_scope != ctx.module {
        walk_children(node, ctx, source, output);
        return;
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        // `var ( a = 1; b = 2 )` groups its specs in a list.
        if child.kind() == "var_spec_list" {
            handle_var_declaration(child, ctx, source, output);
            continue;
        }
        let name = if child.kind() == "var_spec" {
            extract_name_from_spec(child, source)
        } else {
            None
        };
        if let Some(name) = name {
            let signature = var_signature(child, &name, ctx, source);
            let qualname = format!("{}.{}", ctx.module, name);
            let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(child);
            output.symbols.push(SymbolInput {
                kind: "variable".to_string(),
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
                source_qualname: Some(ctx.module.clone()),
                target_qualname: Some(qualname),
                detail: None,
                evidence_snippet: None,
                ..Default::default()
            });
        }
        // The initializer (`var hooks = []func(){ func(){ Helper() } }`,
        // `var x = build()`) is sourced from the module, like the value
        // references in a package-level dispatch table.
        walk_children(child, ctx, source, output);
    }
}

/// What a package-level variable's type is, for a receiver typed through
/// it (`var v *Viper` -> `*Viper`; `var v = New()` -> `= <GoValue>`).
fn var_signature(spec: Node<'_>, name: &str, ctx: &Context, source: &str) -> Option<String> {
    let dir = ctx.module.rsplit_once('/').map_or("", |(dir, _)| dir);
    let scope = LocalTypes::package_scope(dir, ctx.imports.clone());
    if let Some(ty) = spec.child_by_field_name("type") {
        return scope
            .type_text(ty, source)
            .map(|t| crate::model::go_type_marker(&t));
    }
    let mut cursor = spec.walk();
    let names: Vec<Node<'_>> = spec.children_by_field_name("name", &mut cursor).collect();
    let position = names.iter().position(|n| node_text(*n, source) == name)?;
    let values = spec.child_by_field_name("value")?;
    let mut cursor = values.walk();
    let exprs: Vec<Node<'_>> = values.named_children(&mut cursor).collect();
    let value = if exprs.len() == names.len() {
        scope.value_type(exprs[position], 0, source)?
    } else if exprs.len() == 1 {
        scope.value_type(exprs[0], u8::try_from(position).ok()?, source)?
    } else {
        return None;
    };
    Some(crate::model::go_type_marker(&format!(
        "= {}",
        serde_json::to_string(&value).ok()?
    )))
}

fn walk_children(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

fn handle_import(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "import_spec" || child.kind() == "import_spec_list" {
            extract_imports_from_node(child, ctx, source, output);
        }
    }
}

fn extract_imports_from_node(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    if node.kind() == "import_spec" {
        if let Some(path_node) = node.child_by_field_name("path") {
            let import_path = extract_string_literal(path_node, source);
            if !import_path.is_empty() {
                let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
                let snippet =
                    util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
                // The name the file uses for the package: the explicit alias
                // or the path's default (`_` and `.` imports bind none).
                let alias = node
                    .child_by_field_name("name")
                    .map(|n| node_text(n, source))
                    .or_else(|| default_package_name(&import_path))
                    .filter(|a| !matches!(a.as_str(), "_" | "."));
                output.edges.push(EdgeInput {
                    kind: "IMPORTS".to_string(),
                    source_qualname: Some(ctx.module.clone()),
                    target_qualname: Some(import_path),
                    detail: alias,
                    evidence_snippet: snippet,
                    evidence_start_line: Some(start_line),
                    evidence_end_line: Some(end_line),
                    ..Default::default()
                });
            }
        }
    } else {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            extract_imports_from_node(child, ctx, source, output);
        }
    }
}

fn handle_interface_embedding(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    interface_name: &str,
) {
    let Some(type_node) = node.child_by_field_name("type") else {
        return;
    };
    if type_node.kind() != "interface_type" {
        return;
    }

    let mut cursor = type_node.walk();
    for child in type_node.named_children(&mut cursor) {
        if child.kind() == "qualified_type" || child.kind() == "type_identifier" {
            let base_type = node_text(child, source);
            if !base_type.is_empty() {
                let source_qualname = format!("{}.{}", ctx.module, interface_name);
                output.edges.push(EdgeInput {
                    kind: "EXTENDS".to_string(),
                    source_qualname: Some(source_qualname),
                    target_qualname: Some(base_type),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
        }
    }
}

fn handle_call(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    // Try HTTP route detection
    for edge in http_route_edges(node, ctx, source) {
        output.edges.push(edge);
    }
    // Try HTTP call detection
    if let Some(edge) = http_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    // Try gRPC call detection
    if let Some(edge) = grpc_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    // Try channel/bus detection
    if let Some(edge) = channel_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    // Config/env read detection
    if let Some(edge) = config_read_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    // viper.Unmarshal / viper.UnmarshalKey → CONFIG_BIND
    if let Some(edge) = viper_config_bind_edge(node, ctx, source) {
        output.edges.push(edge);
    }

    // General CALLS edge
    let Some(function_node) = node.child_by_field_name("function") else {
        return;
    };
    emit_calls_edge(node, function_node, ctx, source, output);
}

/// The `CALLS` edge of call `node` whose callee expression is `function_node`.
fn emit_calls_edge(
    node: Node<'_>,
    function_node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    // `Map[int](x)` / `pkg.Map[int, string](x)` call `Map`; the type
    // arguments are not part of the callee.
    let function_node = strip_type_arguments(function_node);
    // `func(){..}()` has no name to bind, and a conversion to a predeclared
    // type or a builtin (`len(x)`, `string(b)`) is no call to a declaration.
    if function_node.kind() == "func_literal" {
        return;
    }
    let raw = node_text(function_node, source);
    if raw.is_empty() || is_builtin_callee(function_node, &raw, ctx) {
        return;
    }
    if emit_receiver_call(node, function_node, &raw, ctx, source, output) {
        return;
    }
    let mut target = resolve_call_target(&raw, ctx);
    let mut import_candidates = Vec::new();
    let mut receiver_type = ReceiverType::NotTracked;
    let mut local_call = false;
    match callee_parts(function_node, source) {
        // `pkg.Func(..)` through an import (not rebound locally): the call is
        // bound by the import path, never by the name `Func` alone.
        Some((Some(base), name)) if !ctx.types.is_bound(&base, function_node.start_byte()) => {
            if let Some(path) = ctx.imports.get(&base) {
                target = Some(format!("{path}.{name}"));
                import_candidates.push(format!("{path}|{name}"));
            } else {
                import_candidates.push(format!("{GO_UNKNOWN_QUALIFIER}{base}|{name}"));
            }
        }
        // `Func(..)`: a function of the calling file's package (a sibling
        // file's included), unless a local variable of that name is called.
        Some((None, name)) => {
            if ctx.bound_names.contains(&name) {
                target = None;
                local_call = true;
            } else {
                let dir = ctx.module.rsplit_once('/').map_or("", |(dir, _)| dir);
                import_candidates.push(format!("{GO_SAME_PACKAGE}{dir}|{name}"));
            }
        }
        _ => {}
    }
    // A method called on a call result or other expression (`a.B().C()`,
    // `f()[i].M()`) has no printable receiver. It keeps the method name as
    // its target and an untyped receiver, so the resolver neither guesses a
    // same-named symbol nor drops it: it is stored as an unresolved
    // reference a receiver-typing pass can finish later.
    if target.is_none()
        && !local_call
        && function_node.kind() == "selector_expression"
        && let Some(field) = function_node.child_by_field_name("field")
    {
        target = Some(node_text(field, source));
        receiver_type = ReceiverType::Unresolved;
    }
    // The callee text, whitespace-collapsed, shows the receiver expression
    // of a call whose target is only a method name, or of one with no
    // target (`f()(x)`, a called local).
    let detail = if receiver_type == ReceiverType::Unresolved || target.is_none() {
        Some(collapse_call_target_whitespace(&raw))
    } else {
        None
    };
    output.edges.push(EdgeInput {
        target_qualname: target,
        detail,
        import_candidates,
        receiver_type,
        // A bare identifier callee (`foo()`) vs. a selector expression
        // (`pkg.Func()`, `obj.Method()`) — see `EdgeInput::bare_call`'s
        // doc.
        bare_call: matches!(function_node.kind(), "identifier" | "type_identifier"),
        ..site_edge("CALLS", node, ctx, source)
    });
}

/// A method called on a value (`s.Handle()`, `c.conn.Close()`,
/// `NewB().C()`) rather than through an imported package: the call binds
/// through the receiver's type or not at all, never by the method's name.
/// Returns false for a callee that is no such call (a plain name, or
/// `pkg.Func` through an import).
///
/// A receiver typed in this file (`name T`, `x := T{}`, ...) becomes the
/// package candidate `<place>|T.Method`. One typed through a declaration in
/// another file (`x := NewServer()`, a field) is stored as a [`GoDeferred`]
/// the resolver finishes. One with no nameable type is untyped: the edge
/// keeps the method name and binds nothing.
fn emit_receiver_call(
    node: Node<'_>,
    function_node: Node<'_>,
    raw: &str,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) -> bool {
    if function_node.kind() != "selector_expression" {
        return false;
    }
    let (Some(operand), Some(field)) = (
        function_node.child_by_field_name("operand"),
        function_node.child_by_field_name("field"),
    ) else {
        return false;
    };
    // `pkg.Func(..)`: bound by the import path (or an unknown qualifier).
    if operand.kind() == "identifier"
        && !ctx
            .types
            .is_bound(&node_text(operand, source), operand.start_byte())
    {
        return false;
    }
    let method = node_text(field, source);
    let site = site_edge("CALLS", node, ctx, source);
    let edge = match ctx.types.value_type(operand, 0, source) {
        Some(GoValue::Type { place, name }) => {
            let target = if place.starts_with([GO_SAME_PACKAGE, GO_IMPORTED_PACKAGE]) {
                format!("{name}.{method}")
            } else {
                format!("{place}.{name}.{method}")
            };
            EdgeInput {
                target_qualname: Some(target),
                import_candidates: vec![format!("{place}|{name}.{method}")],
                ..site
            }
        }
        Some(recv) => EdgeInput {
            target_qualname: Some(method.clone()),
            detail: Some(collapse_call_target_whitespace(raw)),
            receiver_type: ReceiverType::GoDeferred(GoDeferred { recv, method }),
            ..site
        },
        None => EdgeInput {
            target_qualname: Some(method),
            detail: Some(collapse_call_target_whitespace(raw)),
            receiver_type: ReceiverType::Unresolved,
            ..site
        },
    };
    output.edges.push(edge);
    true
}

/// An edge of `kind` sourced from the current scope, with the evidence
/// snippet and lines of `node`: the part `CALLS` and `REFERENCES` edges share.
fn site_edge(kind: &str, node: Node<'_>, ctx: &Context, source: &str) -> EdgeInput {
    let (start_line, _, end_line, _, start_byte, end_byte) = span(node);
    EdgeInput {
        kind: kind.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        evidence_snippet: util::edge_evidence_snippet(
            source, start_byte, end_byte, start_line, end_line,
        ),
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    }
}

/// `(qualifier, name)` of a callee that is a plain or package-qualified name:
/// `Func` -> `(None, Func)`, `pkg.Func` -> `(Some(pkg), Func)`. `None` for any
/// other callee (`a.b.C`, `a.B().C`, `f()`).
fn callee_parts(function_node: Node<'_>, source: &str) -> Option<(Option<String>, String)> {
    match function_node.kind() {
        "identifier" | "type_identifier" => Some((None, node_text(function_node, source))),
        "selector_expression" => {
            let operand = function_node.child_by_field_name("operand")?;
            let field = function_node.child_by_field_name("field")?;
            (operand.kind() == "identifier")
                .then(|| (Some(node_text(operand, source)), node_text(field, source)))
        }
        "qualified_type" => Some((
            Some(node_text(
                function_node.child_by_field_name("package")?,
                source,
            )),
            node_text(function_node.child_by_field_name("name")?, source),
        )),
        _ => None,
    }
}

/// The callee of a generic instantiation call without its type arguments.
fn strip_type_arguments(node: Node<'_>) -> Node<'_> {
    let mut node = node;
    while matches!(node.kind(), "index_expression" | "generic_type") {
        match node
            .child_by_field_name("operand")
            .or_else(|| node.child_by_field_name("type"))
        {
            Some(inner) => node = inner,
            None => break,
        }
    }
    node
}

/// Go's predeclared functions. `min`, `max` and `clear` are builtins only
/// from Go 1.21; before that a package commonly declared its own `min` / `max`,
/// which is why the check below yields to a local rebinding (`bound_names`).
/// Kept apart from `GO_PREDECLARED_TYPES` because a builtin is called with
/// arguments of any type, while a predeclared type is *converted to*
/// (`string(b)`); both are "not a call to a declaration of this repository"
/// and share `is_builtin_callee`.
const GO_BUILTINS: &[&str] = &[
    "append", "len", "cap", "make", "new", "delete", "copy", "panic", "recover", "print",
    "println", "close", "min", "max", "clear", "complex", "real", "imag",
];
/// Go's predeclared types: `T(x)` is a conversion, never a call.
const GO_PREDECLARED_TYPES: &[&str] = &[
    "any",
    "bool",
    "byte",
    "comparable",
    "complex64",
    "complex128",
    "error",
    "float32",
    "float64",
    "int",
    "int8",
    "int16",
    "int32",
    "int64",
    "rune",
    "string",
    "uint",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "uintptr",
];

/// A bare builtin call or conversion to a predeclared type the function
/// does not itself rebind. A name the function binds (a parameter, `:=`,
/// `var`, `range` variable) shadows the builtin, so `len := f(); len(1)` is a
/// call of that local, not of the builtin. A package-level `func min()` in
/// another file is not seen (the extractor reads one file), so the builtin
/// reading wins there.
fn is_builtin_callee(function_node: Node<'_>, raw: &str, ctx: &Context) -> bool {
    function_node.kind() == "identifier"
        && !ctx.bound_names.contains(raw)
        && (GO_BUILTINS.contains(&raw) || GO_PREDECLARED_TYPES.contains(&raw))
}

/// Binds the package-qualified references and calls of a file (candidate
/// `<import path>|Name`) to the repository, with the hook's view of the
/// repo's Go modules (`GoRepo`). An import path inside an in-repo module (the
/// nearest or any other `go.mod`'s path, a `replace` target, or a vendored
/// copy) names a package directory: the candidate becomes `><dir>|Name`.
/// Any other import is foreign: a value reference through it is dropped; a
/// call through it keeps its `<import path>|Name` candidate, which the
/// resolver binds to an external stub and never to a same-named repo symbol.
/// Without any `go.mod` nothing is known about the imports, so a call is left
/// to the name tiers as `<last path element>.Name`.
fn bind_import_candidates(
    repo: &GoRepo,
    repo_root: &Path,
    file_rel_path: &str,
    edges: &mut Vec<EdgeInput>,
) {
    bind_deferred_places(repo, repo_root, file_rel_path, edges);
    let starts_with_marker = |e: &EdgeInput, m: char| {
        e.import_candidates
            .first()
            .is_some_and(|c| c.starts_with(m))
    };
    if edges
        .iter()
        .any(|e| e.kind == edge_views::CALLS && starts_with_marker(e, GO_UNKNOWN_QUALIFIER))
    {
        bind_unknown_qualifiers(repo, repo_root, file_rel_path, edges);
    }
    let is_import_edge = |e: &EdgeInput| {
        (e.kind == edge_views::REFERENCES || e.kind == edge_views::CALLS)
            && e.import_candidates
                .first()
                .is_some_and(|c| !c.starts_with([GO_SAME_PACKAGE, GO_IMPORTED_PACKAGE]))
    };
    if !edges.iter().any(is_import_edge) {
        return;
    }
    edges.retain_mut(|edge| {
        if !is_import_edge(edge) {
            return true;
        }
        let is_call = edge.kind == edge_views::CALLS;
        let (import_path, member) = edge.import_candidates[0]
            .split_once('|')
            .unwrap_or_default();
        let (import_path, member) = (import_path.to_string(), member.to_string());
        if !repo.has_modules() {
            // Nothing is known about what the path names: a call falls back
            // to its `<last path element>.Name` text for the name tiers.
            if is_call && member.contains('.') {
                // A method of an imported type: whether the type is the
                // repository's is unknown, so nothing binds it.
                refuse_receiver(edge);
            } else if is_call {
                edge.target_qualname =
                    default_package_name(&import_path).map(|pkg| format!("{pkg}.{member}"));
                edge.import_candidates.clear();
            }
            return is_call;
        }
        // Every candidate that names a package of this repository (several
        // when an unknown qualifier could be any of a file's imports).
        let in_repo: Vec<String> = edge
            .import_candidates
            .iter()
            .filter_map(|candidate| {
                let (path, member) = candidate.split_once('|')?;
                match repo.resolve_import(repo_root, file_rel_path, path) {
                    ImportTarget::InRepo(dir) => {
                        Some(format!("{GO_IMPORTED_PACKAGE}{dir}|{member}"))
                    }
                    ImportTarget::Foreign => None,
                }
            })
            .collect();
        if in_repo.is_empty() {
            // A call keeps its foreign `<import path>|Name` candidate (bound
            // to an external stub); a value reference through it is dropped.
            return is_call;
        }
        edge.import_candidates = in_repo;
        true
    });
}

/// Rewrite the import paths a deferred Go receiver names (`GoValue` places)
/// to the repository package directory they stand for (`><dir>`); a path
/// outside the repository stays a path.
fn bind_deferred_places(
    repo: &GoRepo,
    repo_root: &Path,
    file_rel_path: &str,
    edges: &mut [EdgeInput],
) {
    fn bind(value: &mut GoValue, repo: &GoRepo, repo_root: &Path, file_rel_path: &str) {
        match value {
            GoValue::Type { place, .. } | GoValue::Func { place, .. } => {
                if !place.starts_with([GO_SAME_PACKAGE, GO_IMPORTED_PACKAGE])
                    && repo.has_modules()
                    && let ImportTarget::InRepo(dir) =
                        repo.resolve_import(repo_root, file_rel_path, place)
                {
                    *place = format!("{GO_IMPORTED_PACKAGE}{dir}");
                }
            }
            GoValue::Method { recv, .. } => bind(recv, repo, repo_root, file_rel_path),
            GoValue::Field { owner, .. } => bind(owner, repo, repo_root, file_rel_path),
            GoValue::Seq(inner) | GoValue::Elem(inner) => {
                bind(inner, repo, repo_root, file_rel_path)
            }
            GoValue::Var { .. } => {}
            GoValue::Struct(fields) => {
                for (_, field) in fields {
                    bind(field, repo, repo_root, file_rel_path);
                }
            }
        }
    }
    for edge in edges.iter_mut() {
        if let ReceiverType::GoDeferred(deferred) = &mut edge.receiver_type {
            bind(&mut deferred.recv, repo, repo_root, file_rel_path);
        }
    }
}

/// Leave a method call with an untyped receiver: it keeps its method name
/// and binds nothing.
fn refuse_receiver(edge: &mut EdgeInput) {
    let method = edge
        .import_candidates
        .first()
        .and_then(|c| c.rsplit('.').next())
        .map(str::to_string)
        .or_else(|| {
            edge.target_qualname
                .as_deref()
                .and_then(|t| t.rsplit('.').next())
                .map(str::to_string)
        });
    edge.import_candidates.clear();
    edge.receiver_type = ReceiverType::Unresolved;
    edge.target_qualname = method;
}

/// Resolve the `?qualifier|Name` call candidates of one file. The qualifier
/// names an unaliased import of this repository whose *declared* package name
/// it is (`package validate` in directory `validate_pkgb`); a package whose
/// directory holds no Go file yet could be any name, so it stays a candidate
/// (a later sync retries it once the package exists). Otherwise the
/// qualifier is a package variable or a foreign import and the call is left
/// as plain text for the name tiers.
fn bind_unknown_qualifiers(
    repo: &GoRepo,
    repo_root: &Path,
    file_rel_path: &str,
    edges: &mut [EdgeInput],
) {
    // Declared name -> import paths, and the paths whose name is not known.
    let mut by_declared_name: HashMap<String, Vec<String>> = HashMap::new();
    let mut unnamed: Vec<String> = Vec::new();
    if let Ok(source) = std::fs::read_to_string(repo_root.join(file_rel_path))
        && let Ok(mut extractor) = GoExtractor::new()
        && let Some(tree) = extractor.parser.parse(&source, None)
    {
        for (alias, path) in collect_import_specs(tree.root_node(), &source) {
            if alias.is_some() {
                continue;
            }
            if let ImportTarget::InRepo(dir) = repo.resolve_import(repo_root, file_rel_path, &path)
            {
                match repo.package_name(repo_root, &dir) {
                    Some(name) => by_declared_name.entry(name).or_default().push(path),
                    None => unnamed.push(path),
                }
            }
        }
    }
    for edge in edges.iter_mut() {
        if edge.kind != edge_views::CALLS
            || !edge
                .import_candidates
                .first()
                .is_some_and(|c| c.starts_with(GO_UNKNOWN_QUALIFIER))
        {
            continue;
        }
        let candidate = edge.import_candidates.remove(0);
        edge.import_candidates.clear();
        let Some((qualifier, member)) =
            candidate[GO_UNKNOWN_QUALIFIER.len_utf8()..].split_once('|')
        else {
            continue;
        };
        let paths = by_declared_name
            .get(qualifier)
            .into_iter()
            .flatten()
            .chain(&unnamed);
        edge.import_candidates = paths.map(|path| format!("{path}|{member}")).collect();
        // No import of this repository has that package name: the qualifier
        // is a package variable or a foreign import. Its type is unknown, so
        // the member's name alone must not bind.
        if edge.import_candidates.is_empty() {
            let raw = edge.target_qualname.clone();
            refuse_receiver(edge);
            edge.detail = raw;
            // A package variable of this package (declared in any file of
            // it): the resolver reads its type.
            if let Some(method) = edge.target_qualname.clone() {
                let dir = file_rel_path.rsplit_once('/').map_or("", |(dir, _)| dir);
                edge.receiver_type = ReceiverType::GoDeferred(GoDeferred {
                    recv: GoValue::Var {
                        place: format!("{GO_SAME_PACKAGE}{dir}"),
                        name: qualifier.to_string(),
                    },
                    method,
                });
            }
        }
    }
}

/// `(explicit alias, import path)` of every import of the file, `_` and `.`
/// imports excluded.
fn collect_import_specs(root: Node<'_>, source: &str) -> Vec<(Option<String>, String)> {
    fn walk(node: Node<'_>, source: &str, out: &mut Vec<(Option<String>, String)>) {
        if node.kind() == "import_spec" {
            if let Some(path) = node.child_by_field_name("path") {
                let path = extract_string_literal(path, source);
                let alias = node
                    .child_by_field_name("name")
                    .map(|n| node_text(n, source));
                if !path.is_empty() && !matches!(alias.as_deref(), Some("_" | ".")) {
                    out.push((alias, path));
                }
            }
            return;
        }
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            walk(child, source, out);
        }
    }
    let mut out = Vec::new();
    walk(root, source, &mut out);
    out
}

/// Import alias (or the path's guessed package name) -> import path.
fn collect_import_aliases(root: Node<'_>, source: &str) -> HashMap<String, String> {
    collect_import_specs(root, source)
        .into_iter()
        .filter_map(|(alias, path)| {
            let alias = alias.or_else(|| default_package_name(&path))?;
            Some((alias, path))
        })
        .collect()
}

/// The name an import binds without an alias: its last path element, or the
/// one before a trailing major-version element (`example.com/m/v2` -> `m`).
fn default_package_name(path: &str) -> Option<String> {
    let mut parts = path.rsplit('/');
    let last = parts.next()?;
    let is_major =
        last.len() > 1 && last.starts_with('v') && last[1..].chars().all(|c| c.is_ascii_digit());
    let name = if is_major { parts.next()? } else { last };
    // `gopkg.in/yaml.v3` declares package `yaml`.
    let name = match name.rsplit_once(".v") {
        Some((base, version))
            if !base.is_empty()
                && !version.is_empty()
                && version.chars().all(|c| c.is_ascii_digit()) =>
        {
            base
        }
        _ => name,
    };
    (!name.is_empty()).then(|| name.replace('-', "_"))
}

/// Set the per-function binding state: names the function binds itself and
/// the declared type of its receiver and parameters.
fn enter_function_scope(node: Node<'_>, source: &str, ctx: &mut Context) {
    let mut bound = HashSet::new();
    collect_bound_names(node, source, &mut bound);
    ctx.bound_names = Rc::new(bound);
    let mut types = HashMap::new();
    for field in ["receiver", "parameters"] {
        let Some(list) = node.child_by_field_name(field) else {
            continue;
        };
        let mut cursor = list.walk();
        for param in list.named_children(&mut cursor) {
            let Some(type_node) = param.child_by_field_name("type") else {
                continue;
            };
            let Some(ty) = local_type_name(type_node, source) else {
                continue;
            };
            let mut names = param.walk();
            for name in param.children_by_field_name("name", &mut names) {
                types.insert(node_text(name, source), ty.clone());
            }
        }
    }
    ctx.param_types = Rc::new(types);
    let dir = ctx.module.rsplit_once('/').map_or("", |(dir, _)| dir);
    ctx.types = Rc::new(LocalTypes::collect(node, source, dir, ctx.imports.clone()));
}

/// Every name `node` (a function) binds: parameters (receiver included),
/// `:=` and `var` names, `range` variables and type-switch bindings.
fn collect_bound_names(node: Node<'_>, source: &str, out: &mut HashSet<String>) {
    match node.kind() {
        "parameter_declaration" | "variadic_parameter_declaration" | "var_spec" => {
            let mut cursor = node.walk();
            for name in node.children_by_field_name("name", &mut cursor) {
                collect_identifier_names(name, source, out);
            }
        }
        "short_var_declaration" | "range_clause" => {
            if let Some(left) = node.child_by_field_name("left") {
                collect_identifier_names(left, source, out);
            }
        }
        "type_switch_statement" => {
            if let Some(alias) = node.child_by_field_name("alias") {
                collect_identifier_names(alias, source, out);
            }
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_bound_names(child, source, out);
    }
}

/// `REFERENCES` edge for a function used as a value, not called: an
/// identifier or selector passed as a call argument or given as the value of
/// a composite-literal element (map value, struct field value, slice item).
/// A bare name is a function of the package; `pkg.Name` is a function of an
/// imported package; `recv.Name` is a method value on a receiver, parameter
/// or local whose type is a plain local type. Locals are never references.
fn value_reference_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let parent = node.parent()?;
    let in_value_position = match parent.kind() {
        "argument_list" => parent
            .parent()
            .is_some_and(|c| c.kind() == "call_expression"),
        // A keyed element's key names a field (struct) or index, not a value.
        "literal_element" => parent.parent().is_some_and(|p| {
            p.kind() == "literal_value"
                || (p.kind() == "keyed_element" && p.child_by_field_name("value") == Some(parent))
        }),
        _ => false,
    };
    if !in_value_position || ctx.current_scope.is_empty() {
        return None;
    }
    let dir = ctx.module.rsplit_once('/').map_or("", |(dir, _)| dir);
    let (target, candidate, bare) = if node.kind() == "identifier" {
        let name = node_text(node, source);
        if ctx.bound_names.contains(&name) || name == "_" {
            return None;
        }
        (
            format!("{}.{name}", ctx.module),
            format!("{GO_SAME_PACKAGE}{dir}|{name}"),
            true,
        )
    } else {
        let operand = node.child_by_field_name("operand")?;
        let field = node_text(node.child_by_field_name("field")?, source);
        if operand.kind() != "identifier" {
            return None;
        }
        let base = node_text(operand, source);
        if let Some(path) = ctx
            .imports
            .get(&base)
            .filter(|_| !ctx.bound_names.contains(&base))
        {
            // The import path in the target text keeps a same-named package
            // directory from matching it as an exact qualname.
            (format!("{path}.{field}"), format!("{path}|{field}"), false)
        } else {
            let ty =
                ctx.param_types.get(&base).map(String::as_str).or_else(|| {
                    resolve_var_type_at(&ctx.local_var_types, &base, node.start_byte())
                })?;
            (
                format!("{}.{ty}.{field}", ctx.module),
                format!("{GO_SAME_PACKAGE}{dir}|{ty}.{field}"),
                false,
            )
        }
    };
    Some(EdgeInput {
        target_qualname: Some(target),
        import_candidates: vec![candidate],
        bare_call: bare,
        call_shape: Some(CallShape::value_reference()),
        ..site_edge(edge_views::REFERENCES, node, ctx, source)
    })
}

/// Detect os.Getenv("KEY"), os.LookupEnv("KEY"), viper.GetString("key"), viper.Get("key") → CONFIG_READ
fn config_read_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function_node = node.child_by_field_name("function")?;
    let (receiver, method) = split_selector_expr(function_node, source)?;

    let (framework, source_type) =
        if receiver == "os" && (method == "Getenv" || method == "LookupEnv") {
            ("go", "env")
        } else if receiver == "viper" || receiver.ends_with(".viper") {
            match method.as_str() {
                "GetString" | "Get" | "GetBool" | "GetInt" | "GetFloat64" | "GetDuration" => {
                    ("viper", "config")
                }
                _ => return None,
            }
        } else {
            return None;
        };

    let args = call_arguments(node);
    let key_node = args.first()?;
    let key = extract_string_literal(*key_node, source);
    if key.is_empty() {
        return None;
    }
    let env_uri = config::normalize_env_var_name(&key)?;
    let detail = config::build_config_read_detail(source_type, &env_uri, &key, framework);
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

/// Detect `viper.Unmarshal(&cfg)` and `viper.UnmarshalKey("key", &cfg)` → CONFIG_BIND edges.
/// Resolves the struct type from local variable tracking in the context.
fn viper_config_bind_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function_node = node.child_by_field_name("function")?;
    let (receiver, method) = split_selector_expr(function_node, source)?;

    // Only match viper.Unmarshal or viper.UnmarshalKey
    if receiver != "viper" && !receiver.ends_with(".viper") {
        return None;
    }
    let struct_arg_index = match method.as_str() {
        "Unmarshal" => 0,
        "UnmarshalKey" => 1,
        _ => return None,
    };

    let args = call_arguments(node);
    let struct_arg = args.get(struct_arg_index)?;

    // Argument should be &cfg — a unary_expression with operator "&"
    let var_name = extract_address_of_var(*struct_arg, source)?;

    // Scope-aware lookup: among all declarations of `var_name` whose containing
    // block spans the call site, prefer the innermost (smallest block range).
    // This correctly handles sibling-block shadowing (Finding 1) and closure
    // shadowing (Finding 2).
    let call_byte = node.start_byte();
    let struct_type = resolve_var_type_at(&ctx.local_var_types, &var_name, call_byte)?;

    // Qualify the struct type with the module if it's a simple (unqualified) name
    let target_qualname = if struct_type.contains('.') {
        struct_type.to_string()
    } else {
        format!("{}.{}", ctx.module, struct_type)
    };

    let detail = config::build_config_bind_detail(&target_qualname, &method, "unmarshal", "viper");
    let (start_line, _, end_line, _, _, _) = span(node);
    Some(EdgeInput {
        kind: config::CONFIG_BIND_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(target_qualname),
        detail: Some(detail),
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

/// Extract the variable name from a `&varName` unary expression node.
fn extract_address_of_var(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() != "unary_expression" {
        return None;
    }
    // Check operator is "&"
    let operator = node.child_by_field_name("operator")?;
    if node_text(operator, source) != "&" {
        return None;
    }
    let operand = node.child_by_field_name("operand")?;
    if operand.kind() == "identifier" {
        let name = node_text(operand, source);
        if !name.is_empty() {
            return Some(name);
        }
    }
    None
}

/// Collect local variable declarations with their containing-block byte ranges.
/// Handles:
///   `var cfg DatabaseConfig` (var_declaration → var_spec with explicit type)
///   `cfg := DatabaseConfig{}` (short_var_declaration with composite_literal)
///
/// Each declaration is tagged with the byte range of the innermost block or
/// func_literal that directly contains it, enabling scope-aware resolution at
/// the call site (see `resolve_var_type_at`).
fn collect_local_var_types(node: Node<'_>, source: &str) -> Vec<ScopedVarType> {
    let mut types = Vec::new();
    let block_start = node.start_byte();
    let block_end = node.end_byte();
    collect_local_var_types_inner(node, source, block_start, block_end, &mut types);
    types
}

fn collect_local_var_types_inner(
    node: Node<'_>,
    source: &str,
    block_start: usize,
    block_end: usize,
    types: &mut Vec<ScopedVarType>,
) {
    match node.kind() {
        "var_declaration" => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if child.kind() == "var_spec" {
                    collect_var_spec_types_scoped(child, source, block_start, block_end, types);
                }
            }
        }
        "short_var_declaration" => {
            collect_short_var_types_scoped(node, source, block_start, block_end, types);
        }
        _ => {}
    }
    // Recurse into all children. When we cross into a new block or func_literal,
    // update the current block range so declarations inside are tagged correctly.
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        let (child_block_start, child_block_end) =
            if child.kind() == "block" || child.kind() == "func_literal" {
                (child.start_byte(), child.end_byte())
            } else {
                (block_start, block_end)
            };
        collect_local_var_types_inner(child, source, child_block_start, child_block_end, types);
    }
}

/// Scope-aware variable type resolver.
///
/// Among all entries for `var_name` whose block range contains `call_byte`,
/// return the type from the innermost (narrowest) block. Returns `None` when:
/// - no declaration for `var_name` exists at the call site, OR
/// - multiple declarations exist in the same innermost block with *different*
///   types (ambiguous — prefer a missed edge over a wrong one).
fn resolve_var_type_at<'a>(
    scoped: &'a [ScopedVarType],
    var_name: &str,
    call_byte: usize,
) -> Option<&'a str> {
    // Candidates: declarations whose block encloses the call site.
    let candidates: Vec<&ScopedVarType> = scoped
        .iter()
        .filter(|sv| sv.name == var_name && sv.block_start <= call_byte && call_byte < sv.block_end)
        .collect();

    if candidates.is_empty() {
        return None;
    }

    // Innermost block = the one with the largest start byte (tightest enclosure).
    let innermost_start = candidates
        .iter()
        .map(|sv| sv.block_start)
        .max()
        .unwrap_or(0);

    let innermost: Vec<&ScopedVarType> = candidates
        .iter()
        .filter(|sv| sv.block_start == innermost_start)
        .copied()
        .collect();

    // All innermost entries must agree on the type.
    let first_type = innermost[0].type_name.as_str();
    if innermost.iter().all(|sv| sv.type_name == first_type) {
        Some(first_type)
    } else {
        // Ambiguous — skip rather than emit a wrong edge.
        None
    }
}

/// Record scoped name → type entries from a `var_spec` node.
/// Handles `var a, b TypeName` and `var cfg = TypeName{}`.
fn collect_var_spec_types_scoped(
    node: Node<'_>,
    source: &str,
    block_start: usize,
    block_end: usize,
    types: &mut Vec<ScopedVarType>,
) {
    let mut cursor = node.walk();
    let names: Vec<String> = node
        .children_by_field_name("name", &mut cursor)
        .map(|name_node| node_text(name_node, source))
        .filter(|name| !name.is_empty())
        .collect();

    let type_name = match node.child_by_field_name("type") {
        Some(type_node) => named_type_text(type_node, source),
        // `var cfg = TypeName{}` — infer from the initializer, but only for a
        // single name; multiple initializers would need positional pairing.
        None if names.len() == 1 => node
            .child_by_field_name("value")
            .and_then(|value| extract_composite_literal_type(value, source)),
        None => None,
    };
    let Some(type_name) = type_name else {
        return;
    };
    for name in names {
        types.push(ScopedVarType {
            name,
            type_name: type_name.clone(),
            block_start,
            block_end,
        });
    }
}

/// Record scoped name → type entries from `cfg := TypeName{}` (short_var_declaration),
/// pairing each left-hand identifier with its right-hand expression positionally.
fn collect_short_var_types_scoped(
    node: Node<'_>,
    source: &str,
    block_start: usize,
    block_end: usize,
    types: &mut Vec<ScopedVarType>,
) {
    let Some(left) = node.child_by_field_name("left") else {
        return;
    };
    let Some(right) = node.child_by_field_name("right") else {
        return;
    };
    let mut left_cursor = left.walk();
    let mut right_cursor = right.walk();
    let names = left.named_children(&mut left_cursor);
    let values = right.named_children(&mut right_cursor);
    for (name_node, value_node) in names.zip(values) {
        if name_node.kind() != "identifier" {
            continue;
        }
        let name = node_text(name_node, source);
        if name.is_empty() {
            continue;
        }
        if let Some(type_name) = extract_composite_literal_type(value_node, source) {
            types.push(ScopedVarType {
                name,
                type_name,
                block_start,
                block_end,
            });
        }
    }
}

/// Extract the named type from a composite literal `TypeName{...}`,
/// drilling through an expression_list wrapper if present.
fn extract_composite_literal_type(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "expression_list" {
        let mut cursor = node.walk();
        let first = node.named_children(&mut cursor).next()?;
        return extract_composite_literal_type(first, source);
    }
    // `&Config{}` is typed like `Config{}` (the pointer is unwrapped, as for
    // a declared `*Config`).
    if node.kind() == "unary_expression" {
        return extract_composite_literal_type(node.child_by_field_name("operand")?, source);
    }
    if node.kind() != "composite_literal" {
        return None;
    }
    named_type_text(node.child_by_field_name("type")?, source)
}

/// Text of a named type (`TypeName` or `pkg.TypeName`), unwrapping pointer
/// indirection. Returns None for slice/map/array/chan/func types, which
/// cannot name a bindable config struct.
fn named_type_text(node: Node<'_>, source: &str) -> Option<String> {
    match node.kind() {
        "pointer_type" => {
            let mut cursor = node.walk();
            let inner = node.named_children(&mut cursor).next()?;
            named_type_text(inner, source)
        }
        "type_identifier" | "qualified_type" => {
            let text = node_text(node, source);
            if text.is_empty() { None } else { Some(text) }
        }
        _ => None,
    }
}

fn http_route_edges(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    let Some(function_node) = node.child_by_field_name("function") else {
        return edges;
    };

    let Some((receiver, method_name)) = split_selector_expr(function_node, source) else {
        return edges;
    };

    // Check if method is an HTTP verb or HandleFunc
    let method_lower = method_name.to_ascii_lowercase();
    let is_http_verb = http::normalize_method(&method_name).is_some();
    let is_handle = method_lower == "handlefunc" || method_lower == "handle";

    if !is_http_verb && !is_handle {
        return edges;
    }

    // Extract arguments
    let args = call_arguments(node);
    if args.is_empty() {
        return edges;
    }

    // First argument should be the path
    let raw_path = extract_string_literal(args[0], source);
    if raw_path.is_empty() || !raw_path.starts_with('/') {
        return edges;
    }

    let method = if is_http_verb {
        match http::normalize_method(&method_name) {
            Some(m) => m,
            None => return edges,
        }
    } else {
        http::HTTP_ANY.to_string()
    };

    let normalized = match http::normalize_path(&raw_path) {
        Some(n) => n,
        None => return edges,
    };
    let framework = detect_framework(&receiver);
    let detail = http::build_route_detail(&method, &normalized, &raw_path, framework);
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);

    edges.push(EdgeInput {
        kind: http::HTTP_ROUTE_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    });

    edges
}

fn http_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function_node = node.child_by_field_name("function")?;
    let (receiver, method_name) = split_selector_expr(function_node, source)?;

    // Check if it's an HTTP client call
    let method = http::normalize_method(&method_name)?;

    let args = call_arguments(node);
    if args.is_empty() {
        return None;
    }

    let raw_path = extract_string_literal(args[0], source);
    if raw_path.is_empty() {
        return None;
    }

    // Only consider it an HTTP call if receiver looks like a client or is http package
    let receiver_lower = receiver.to_ascii_lowercase();
    if !receiver_lower.contains("http")
        && !receiver_lower.contains("client")
        && !receiver_lower.contains("request")
    {
        return None;
    }

    let normalized = http::normalize_path(&raw_path)?;
    let client = if receiver_lower.contains("http") {
        "http"
    } else {
        "http_client"
    };
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
    receiver_type: &str,
) -> Option<EdgeInput> {
    // Check if the receiver type is a known gRPC server
    let server_info = ctx.grpc_servers.get(receiver_type)?;
    let (raw_path, normalized) = proto::normalize_rpc_path(None, &server_info.service, rpc_name)?;
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    let detail = json!({
        "framework": "grpc-go",
        "role": "server",
        "service": server_info.service.as_str(),
        "rpc": rpc_name,
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
    let function_node = node.child_by_field_name("function")?;
    let (receiver, method_name) = split_selector_expr(function_node, source)?;

    // Check if receiver is a known gRPC client
    let client_info = ctx.grpc_clients.get(&receiver)?;
    let (raw_path, normalized) =
        proto::normalize_rpc_path(None, &client_info.service, &method_name)?;
    let detail = json!({
        "framework": "grpc-go",
        "role": "client",
        "service": client_info.service.as_str(),
        "rpc": method_name,
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

fn channel_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let function_node = node.child_by_field_name("function")?;
    let (receiver, method_name) = split_selector_expr(function_node, source)?;

    if !channel::is_bus_receiver(&receiver) {
        return None;
    }

    let kind = if channel::is_publish_method(&method_name) {
        channel::CHANNEL_PUBLISH_KIND
    } else if channel::is_subscribe_method(&method_name) {
        channel::CHANNEL_SUBSCRIBE_KIND
    } else {
        return None;
    };

    let args = call_arguments(node);
    let raw_topic = node_text(*args.first()?, source);
    let normalized =
        channel::resolve_topic(&raw_topic, &ctx.string_consts, &LocalBinding::NotLocal)?;
    let detail = if kind == channel::CHANNEL_PUBLISH_KIND {
        channel::build_publish_detail(&normalized, &raw_topic, "go-bus")
    } else {
        channel::build_subscribe_detail(&normalized, &raw_topic, "go-bus")
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

fn collect_grpc_clients(node: Node<'_>, source: &str) -> HashMap<String, GrpcClientInfo> {
    let mut clients = HashMap::new();
    collect_grpc_clients_inner(node, source, &mut clients);
    clients
}

fn collect_grpc_clients_inner(
    node: Node<'_>,
    source: &str,
    clients: &mut HashMap<String, GrpcClientInfo>,
) {
    // Look for short_var_declaration: client := pb.NewGreeterClient(conn)
    if node.kind() == "short_var_declaration"
        && let Some((name, service)) = grpc_client_from_short_var(node, source)
    {
        clients.insert(name, service);
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_grpc_clients_inner(child, source, clients);
    }
}

fn grpc_client_from_short_var(node: Node<'_>, source: &str) -> Option<(String, GrpcClientInfo)> {
    let left = node.child_by_field_name("left")?;
    let right = node.child_by_field_name("right")?;

    let var_name = extract_first_identifier(left, source)?;
    let service = extract_grpc_service_from_client_call(right, source)?;

    Some((var_name, GrpcClientInfo { service }))
}

fn extract_first_identifier(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "identifier" {
        let name = node_text(node, source);
        if !name.is_empty() {
            return Some(name);
        }
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(name) = extract_first_identifier(child, source) {
            return Some(name);
        }
    }
    None
}

fn extract_grpc_service_from_client_call(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "call_expression" {
        let function = node.child_by_field_name("function")?;
        let (_receiver, method_name) = split_selector_expr(function, source)?;
        // Check for NewXxxClient pattern
        if method_name.starts_with("New") && method_name.ends_with("Client") {
            let service = &method_name[3..method_name.len() - 6];
            if !service.is_empty() {
                return Some(service.to_string());
            }
        }
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(service) = extract_grpc_service_from_client_call(child, source) {
            return Some(service);
        }
    }
    None
}

fn is_grpc_server_struct(struct_node: Node<'_>, source: &str) -> bool {
    if struct_node.kind() != "struct_type" {
        return false;
    }

    // Find the field list - try field_declaration_list directly
    let mut cursor = struct_node.walk();
    let children: Vec<_> = struct_node.named_children(&mut cursor).collect();

    let field_list = if let Some(fl) = struct_node.child_by_field_name("fields") {
        fl
    } else {
        // Try finding field_declaration_list directly
        let mut found = None;
        for child in &children {
            if child.kind() == "field_declaration_list" {
                found = Some(*child);
                break;
            }
        }
        match found {
            Some(fl) => fl,
            None => return false,
        }
    };

    let mut cursor = field_list.walk();
    for child in field_list.named_children(&mut cursor) {
        // In Go, embedded fields don't have field names, just types
        let type_text = if child.kind() == "field_declaration" {
            // Could be: name type OR just type (embedded)
            if let Some(type_node) = child.child_by_field_name("type") {
                node_text(type_node, source)
            } else {
                // Might be an embedded field - get the first child
                node_text(child, source)
            }
        } else {
            continue;
        };

        if type_text.contains("Unimplemented") && type_text.contains("Server") {
            return true;
        }
    }

    false
}

fn extract_grpc_service_from_server_name(name: &str) -> Option<String> {
    if !name.ends_with("Server") {
        return None;
    }
    let service = &name[..name.len() - 6];
    if service.is_empty() {
        None
    } else {
        Some(service.to_string())
    }
}

fn extract_receiver_type(node: Node<'_>, source: &str) -> Option<String> {
    let receiver = node.child_by_field_name("receiver")?;
    let mut cursor = receiver.walk();
    for child in receiver.named_children(&mut cursor) {
        if child.kind() == "parameter_declaration"
            && let Some(type_node) = child.child_by_field_name("type")
        {
            // The pointer and the type arguments are not part of the type's
            // name: `func (s *Set[T]) Add` is a method of `Set`.
            let type_text = match local_type_name(type_node, source) {
                Some(name) => name,
                None => node_text(type_node, source)
                    .trim_start_matches('*')
                    .trim()
                    .to_string(),
            };
            if !type_text.is_empty() {
                return Some(type_text);
            }
        }
    }
    None
}

/// The name of the plain type `T`, `*T` or `T[A, B]` a declaration names
/// (`None` for a qualified, slice, map or other type).
fn local_type_name(type_node: Node<'_>, source: &str) -> Option<String> {
    let mut node = type_node;
    loop {
        match node.kind() {
            "type_identifier" => return Some(node_text(node, source)),
            "pointer_type" | "parenthesized_type" => {
                let mut cursor = node.walk();
                node = node.named_children(&mut cursor).next()?;
            }
            "generic_type" => node = node.child_by_field_name("type")?,
            _ => return None,
        }
    }
}

/// `{name Type; Embedded; ..}` of a struct type: each field's declared type
/// as written (an embedded one marked `~`), for the resolver to type
/// `x.field.Method()` and to find methods promoted from embedded types.
fn struct_signature(struct_node: Node<'_>, source: &str) -> Option<String> {
    const MAX_LEN: usize = 4000;
    let list = struct_node
        .named_children(&mut struct_node.walk())
        .find(|c| c.kind() == "field_declaration_list")?;
    let mut fields: Vec<String> = Vec::new();
    let mut cursor = list.walk();
    for field in list.named_children(&mut cursor) {
        if field.kind() != "field_declaration" {
            continue;
        }
        let Some(type_node) = field.child_by_field_name("type") else {
            continue;
        };
        let ty = node_text(type_node, source)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        // An anonymous struct, interface or function type names no type.
        let ty = if ty.contains(['{', '(']) {
            "?".to_string()
        } else {
            ty
        };
        let mut names = field.walk();
        let names: Vec<String> = field
            .children_by_field_name("name", &mut names)
            .map(|n| node_text(n, source))
            .collect();
        if names.is_empty() {
            fields.push(format!("~{ty}"));
        } else {
            fields.extend(names.into_iter().map(|n| format!("{n} {ty}")));
        }
    }
    if fields.is_empty() {
        return None;
    }
    let mut out = String::from("{");
    for (i, field) in fields.iter().enumerate() {
        if out.len() + field.len() > MAX_LEN {
            break;
        }
        if i > 0 {
            out.push_str("; ");
        }
        out.push_str(field);
    }
    out.push('}');
    Some(crate::model::go_type_marker(&out))
}

fn extract_function_signature(node: Node<'_>, source: &str) -> Option<String> {
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(n, source));
    let result = node
        .child_by_field_name("result")
        .map(|n| node_text(n, source));
    match (params, result) {
        (Some(p), Some(r)) => Some(format!("{} -> {}", p, r)),
        (Some(p), None) => Some(p),
        _ => None,
    }
}

fn extract_name_from_spec(node: Node<'_>, source: &str) -> Option<String> {
    let name_node = node.child_by_field_name("name")?;
    let name = node_text(name_node, source);
    if name.is_empty() { None } else { Some(name) }
}

fn split_selector_expr(node: Node<'_>, source: &str) -> Option<(String, String)> {
    if node.kind() == "selector_expression" {
        let operand = node.child_by_field_name("operand")?;
        let field = node.child_by_field_name("field")?;
        let receiver = node_text(operand, source);
        let method = node_text(field, source);
        return Some((receiver, method));
    }
    None
}

fn call_arguments(node: Node<'_>) -> Vec<Node<'_>> {
    let mut args = Vec::new();
    let Some(arg_list) = node.child_by_field_name("arguments") else {
        return args;
    };
    let mut cursor = arg_list.walk();
    for child in arg_list.named_children(&mut cursor) {
        // tree-sitter-go emits `comment` as a named child of argument_list;
        // skip it so positional indexing (args[0], args[1]) is not disturbed.
        if child.kind() == "comment" {
            continue;
        }
        args.push(child);
    }
    args
}

fn extract_string_literal(node: Node<'_>, source: &str) -> String {
    let raw = node_text(node, source);
    unquote_go_string(&raw).unwrap_or(raw)
}

fn unquote_go_string(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Interpreted string: "..."
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        return Some(trimmed[1..trimmed.len() - 1].to_string());
    }
    // Raw string: `...`
    if trimmed.starts_with('`') && trimmed.ends_with('`') && trimmed.len() >= 2 {
        return Some(trimmed[1..trimmed.len() - 1].to_string());
    }
    None
}

fn detect_framework(receiver: &str) -> &'static str {
    let lower = receiver.to_ascii_lowercase();
    if lower.contains("gin") {
        "gin"
    } else if lower.contains("echo") {
        "echo"
    } else if lower.contains("chi") {
        "chi"
    } else if lower.contains("mux") {
        "gorilla"
    } else {
        "net/http"
    }
}

fn resolve_call_target(raw: &str, ctx: &Context) -> Option<String> {
    let raw = collapse_call_target_whitespace(raw);
    let raw = raw.as_str();
    if raw.is_empty() || !is_simple_call_target(raw) {
        return None;
    }
    // If it contains a dot, it's a qualified call
    if raw.contains('.') {
        let parts: Vec<&str> = raw.split('.').collect();
        if parts.len() >= 2 {
            // Assume module-level function for now
            return Some(raw.to_string());
        }
    }
    // Simple name - qualify with current module
    Some(format!("{}.{}", ctx.module, raw))
}

fn is_simple_call_target(raw: &str) -> bool {
    raw.chars()
        .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.')
}

#[cfg(test)]
mod tests {
    use super::GoExtractor;
    use crate::indexer::channel;
    use crate::indexer::config;
    use crate::indexer::extract::LanguageExtractor;
    use crate::indexer::http;
    use crate::indexer::proto;

    /// `(source scope, target qualname, detail)` of every CALLS edge.
    fn call_edges(source: &str) -> Vec<(String, Option<String>, Option<String>)> {
        let mut extractor = GoExtractor::new().unwrap();
        let out = extractor.extract(source, "pkg/file").unwrap();
        out.edges
            .into_iter()
            .filter(|e| e.kind == "CALLS")
            .map(|e| (e.source_qualname.unwrap(), e.target_qualname, e.detail))
            .collect()
    }

    #[test]
    fn nested_chained_and_closure_calls_are_recorded() {
        let source = r#"
package pkg

func F(t T) {
    Outer(Inner(len(x)))
    a.
        B().
        C()
    go func() { InGo() }()
    defer func() { InDefer() }()
    t.Run("n", func(t T) { InRun() })
    Map[int](1)
    _ = string(b)
    _ = append(xs, make([]int, 1))
}

var hooks = []func(){ func() { InVar() } }
var built = Build()
var (
    grouped = Group()
)
"#;
        let edges = call_edges(source);
        let targets = |scope: &str| -> Vec<String> {
            let mut v: Vec<String> = edges
                .iter()
                .filter(|(s, ..)| s == scope)
                .filter_map(|(_, t, d)| t.clone().or_else(|| d.clone()))
                .collect();
            v.sort();
            v
        };
        assert_eq!(
            targets("pkg/file.F"),
            [
                "C",
                // `t` is a parameter of the local type `T`.
                "T.Run",
                "a.B",
                "pkg/file.InDefer",
                "pkg/file.InGo",
                "pkg/file.InRun",
                "pkg/file.Inner",
                "pkg/file.Map",
                "pkg/file.Outer",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
        );
        // The chain's method call keeps the method name and its receiver text.
        assert!(
            edges
                .iter()
                .any(|(_, t, d)| { t.as_deref() == Some("C") && d.as_deref() == Some("a.B().C") })
        );
        assert_eq!(
            targets("pkg/file"),
            ["pkg/file.Build", "pkg/file.Group", "pkg/file.InVar"]
        );
    }

    /// `(target, candidates)` of every CALLS edge after the import hook ran
    /// in a repo whose `go.mod` (if any) is `go_mod`.
    fn bound_calls(go_mod: Option<&str>, source: &str) -> Vec<(Option<String>, Vec<String>)> {
        let dir = tempfile::tempdir().unwrap();
        if let Some(go_mod) = go_mod {
            std::fs::write(dir.path().join("go.mod"), go_mod).unwrap();
        }
        let mut extractor = GoExtractor::new().unwrap();
        let mut out = extractor.extract(source, "pkg/file").unwrap();
        extractor.resolve_imports(dir.path(), "pkg/file.go", "pkg/file", &mut out.edges);
        out.edges
            .into_iter()
            .filter(|e| e.kind == "CALLS")
            .map(|e| (e.target_qualname, e.import_candidates))
            .collect()
    }

    /// `(method name or target, package candidates, receiver column)` of the
    /// method calls in `source`, in source order.
    fn receiver_calls(source: &str) -> Vec<(String, Vec<String>, String)> {
        let mut extractor = GoExtractor::new().unwrap();
        let out = extractor.extract(source, "pkg/file").unwrap();
        out.edges
            .into_iter()
            .filter(|e| e.kind == "CALLS")
            .filter(|e| e.detail.is_some() || !e.import_candidates.is_empty())
            .map(|e| {
                let columns = e.receiver_type.to_columns();
                let receiver = match (&columns.receiver_type, &columns.deferred) {
                    (Some(t), _) if t.is_empty() => "untyped".to_string(),
                    (_, Some(_)) => "deferred".to_string(),
                    _ => "typed".to_string(),
                };
                (
                    e.target_qualname.unwrap_or_default(),
                    e.import_candidates,
                    receiver,
                )
            })
            .collect()
    }

    #[test]
    fn receivers_are_typed_from_their_declarations() {
        let source = r#"
package pkg

import "net"

type S struct{ c net.Conn }

func (s *S) M(p *Other, q net.Conn, n int) {
    s.A()
    p.B()
    q.C()
    n.D()
    s.c.E()
    a := &S{}
    a.F()
    var b S
    b.G()
    c := x.(*S)
    c.H()
    switch v := y.(type) {
    case *S:
        v.I()
    case *Other, *S:
        v.J()
    }
}
"#;
        let calls = receiver_calls(source);
        let call = |name: &str| calls.iter().find(|(t, ..)| t.ends_with(name)).unwrap();
        assert_eq!(call("S.A").1, ["=pkg|S.A"]);
        assert_eq!(call("Other.B").1, ["=pkg|Other.B"]);
        assert_eq!(call("net.Conn.C").1, ["net|Conn.C"]);
        // A predeclared type has no methods of the repo's.
        assert_eq!((call("D").1.len(), call("D").2.as_str()), (0, "untyped"));
        // A field is typed through the struct declaration: deferred.
        assert_eq!(call("E").2, "deferred");
        assert_eq!(call("S.F").1, ["=pkg|S.F"]);
        assert_eq!(call("S.G").1, ["=pkg|S.G"]);
        assert_eq!(call("S.H").1, ["=pkg|S.H"]);
        assert_eq!(call("S.I").1, ["=pkg|S.I"]);
        // Two types in one case: the variable has the switch value's type.
        assert_eq!(call("J").2, "untyped");
    }

    #[test]
    fn a_shadowing_binding_hides_the_outer_type() {
        let source = r#"
package pkg

func F(s *S) {
    s.A()
    {
        s := load()
        s.B()
    }
    for s := range items {
        s.C()
    }
    s.D()
}
"#;
        let calls = receiver_calls(source);
        let by = |name: &str| {
            calls
                .iter()
                .find(|(t, ..)| t == name || t.ends_with(&format!(".{name}")))
                .unwrap()
                .clone()
        };
        assert_eq!(by("S.A").1, ["=pkg|S.A"]);
        assert_eq!(by("B").2, "deferred");
        assert_eq!(by("C").2, "untyped");
        assert_eq!(by("S.D").1, ["=pkg|S.D"]);
    }

    #[test]
    fn generic_receivers_are_methods_of_the_type_without_its_arguments() {
        let mut extractor = GoExtractor::new().unwrap();
        let out = extractor
            .extract(
                "package pkg\n\ntype Set[T comparable] struct{ m map[T]bool }\n\nfunc (s *Set[T]) Add(v T) { s.Has(v) }\n",
                "pkg/file",
            )
            .unwrap();
        let names: Vec<&str> = out.symbols.iter().map(|s| s.qualname.as_str()).collect();
        assert!(names.contains(&"pkg/file.Set.Add"), "{names:?}");
        let call = out.edges.iter().find(|e| e.kind == "CALLS").unwrap();
        assert_eq!(call.import_candidates, ["=pkg|Set.Has"]);
        let set = out
            .symbols
            .iter()
            .find(|s| s.qualname == "pkg/file.Set")
            .unwrap();
        assert_eq!(set.signature.as_deref(), Some("#[go_type({m map[T]bool})]"));
    }

    const IMPORTING: &str = r#"
package pkg

import (
    "path/filepath"
    yaml "gopkg.in/yaml.v3"
    "example.com/m/lib"
    "example.com/m"
)

func F(lib2 int) {
    filepath.Join("a")
    yaml.Marshal(1)
    lib.Do()
    m.Root()
    Sibling()
    lib2.X()
    local := 1
    local()
}
"#;

    #[test]
    fn calls_carry_package_candidates_for_imports_and_siblings() {
        let calls = bound_calls(Some("module example.com/m\n"), IMPORTING);
        let c = |t: &str, cand: &[&str]| {
            (
                Some(t.to_string()),
                cand.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            )
        };
        assert_eq!(
            calls,
            [
                // Foreign imports keep their path (bound to an external stub).
                c("path/filepath.Join", &["path/filepath|Join"]),
                c("gopkg.in/yaml.v3.Marshal", &["gopkg.in/yaml.v3|Marshal"]),
                // In-module imports name the package directory.
                c("example.com/m/lib.Do", &[">lib|Do"]),
                c("example.com/m.Root", &[">|Root"]),
                // A bare name is a function of the calling file's package.
                c("pkg/file.Sibling", &["=pkg|Sibling"]),
                // A local receiver is no import and an `int` has no methods:
                // only the method's name is kept, and it binds nothing.
                c("X", &[]),
                (None, vec![]),
            ]
        );
    }

    #[test]
    fn without_a_go_mod_imports_are_left_to_the_name_tiers() {
        let calls = bound_calls(None, IMPORTING);
        assert_eq!(
            &calls[..4],
            [
                (Some("filepath.Join".to_string()), vec![]),
                (Some("yaml.Marshal".to_string()), vec![]),
                (Some("lib.Do".to_string()), vec![]),
                (Some("m.Root".to_string()), vec![]),
            ]
        );
    }

    #[test]
    fn rebound_builtin_name_is_a_call_of_a_local() {
        let edges = call_edges("package pkg\nfunc F() {\n    len := g()\n    len(1)\n}\n");
        // Recorded (not dropped as a builtin) but unresolved: it calls the
        // local, never a package-level `len`.
        assert!(
            edges
                .iter()
                .any(|(_, t, d)| t.is_none() && d.as_deref() == Some("len"))
        );
    }

    #[test]
    fn extracts_http_route_and_call() {
        let source = r#"
package main

import (
    "net/http"
    "github.com/gin-gonic/gin"
)

func main() {
    r := gin.Default()
    r.GET("/api/users/:id", getUser)
    r.POST("/api/users", createUser)

    http.HandleFunc("/health", healthCheck)
    http.Get("http://example.com/api/data")
}

func getUser(c *gin.Context) {}
func createUser(c *gin.Context) {}
func healthCheck(w http.ResponseWriter, r *http.Request) {}
"#;
        let mut extractor = GoExtractor::new().unwrap();
        let file = extractor.extract(source, "main").unwrap();
        let routes: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == http::HTTP_ROUTE_KIND)
            .collect();
        let calls: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == http::HTTP_CALL_KIND)
            .collect();
        assert!(
            routes
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("/api/users/{}")),
            "Expected /api/users/{{}} route, got: {:?}",
            routes
        );
        assert!(
            routes
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("/health")),
            "Expected /health route"
        );
        assert!(!calls.is_empty(), "Expected at least one HTTP call");
    }

    #[test]
    fn extracts_grpc_impl_and_call() {
        let source = r#"
package main

type GreeterServer struct {
    pb.UnimplementedGreeterServer
}

func (s *GreeterServer) SayHello(ctx context.Context, req *pb.HelloRequest) (*pb.HelloReply, error) {
    return &pb.HelloReply{}, nil
}

func run() {
    client := pb.NewGreeterClient(conn)
    client.SayHello(ctx, req)
}
"#;
        let mut extractor = GoExtractor::new().unwrap();
        let file = extractor.extract(source, "main").unwrap();
        let impls: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == proto::RPC_IMPL_KIND)
            .collect();
        let calls: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == proto::RPC_CALL_KIND)
            .collect();
        assert!(!impls.is_empty(), "should have RPC_IMPL edges");
        assert!(!calls.is_empty(), "should have RPC_CALL edges");
    }

    #[test]
    fn extracts_channel_publish_subscribe() {
        let source = r#"
package main

func publisher() {
    bus.Publish("Topics.UserCreated", event)
}

func subscriber() {
    bus.Subscribe("Topics.UserCreated", handler)
}
"#;
        let mut extractor = GoExtractor::new().unwrap();
        let file = extractor.extract(source, "main").unwrap();
        let pub_edges: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == channel::CHANNEL_PUBLISH_KIND)
            .collect();
        let sub_edges: Vec<_> = file
            .edges
            .iter()
            .filter(|e| e.kind == channel::CHANNEL_SUBSCRIBE_KIND)
            .collect();
        assert!(!pub_edges.is_empty(), "should have CHANNEL_PUBLISH edges");
        assert!(!sub_edges.is_empty(), "should have CHANNEL_SUBSCRIBE edges");
    }

    /// Extract `source` as module "main" and return only the CONFIG_BIND edges.
    fn config_bind_edges(source: &str) -> Vec<crate::indexer::extract::EdgeInput> {
        let mut extractor = GoExtractor::new().unwrap();
        let file = extractor.extract(source, "main").unwrap();
        file.edges
            .into_iter()
            .filter(|e| e.kind == config::CONFIG_BIND_KIND)
            .collect()
    }

    /// Every local declaration form that should bind `cfg` to `DatabaseConfig`:
    /// explicit type, composite-literal initializers, multi-name var specs,
    /// and positional short var declarations.
    #[test]
    fn viper_unmarshal_resolves_local_declaration_forms() {
        let declarations = [
            "var cfg DatabaseConfig",
            "cfg := DatabaseConfig{}",
            "var cfg = DatabaseConfig{}",
            "var primary, cfg DatabaseConfig",
            r#"name, cfg := "prod", DatabaseConfig{}"#,
        ];
        for declaration in declarations {
            let source = format!(
                r#"
package main

type DatabaseConfig struct {{
    Host string
}}

func loadConfig() {{
    {declaration}
    viper.Unmarshal(&cfg)
}}
"#
            );
            let bind_edges = config_bind_edges(&source);
            assert!(
                bind_edges
                    .iter()
                    .any(|e| e.target_qualname.as_deref() == Some("main.DatabaseConfig")),
                "`{declaration}`: expected CONFIG_BIND edge to main.DatabaseConfig, got: {bind_edges:?}"
            );
        }
    }

    #[test]
    fn viper_unmarshal_key_emits_config_bind_edge_with_detail() {
        let source = r#"
package main

type AppConfig struct {
    Debug bool
}

func loadConfig() {
    var appCfg AppConfig
    viper.UnmarshalKey("app", &appCfg)
}
"#;
        let bind_edges = config_bind_edges(source);
        let edge = bind_edges
            .iter()
            .find(|e| e.target_qualname.as_deref() == Some("main.AppConfig"))
            .unwrap_or_else(|| {
                panic!("Expected CONFIG_BIND edge to main.AppConfig, got: {bind_edges:?}")
            });
        let detail: serde_json::Value =
            serde_json::from_str(edge.detail.as_deref().unwrap()).unwrap();
        assert_eq!(detail["wrapper_type"], "UnmarshalKey");
        assert_eq!(detail["binding_kind"], "unmarshal");
        assert_eq!(detail["framework"], "viper");
    }

    #[test]
    fn viper_unmarshal_keeps_qualified_type_unprefixed() {
        let source = r#"
package main

func loadConfig() {
    var cfg settings.Database
    viper.Unmarshal(&cfg)
}
"#;
        let bind_edges = config_bind_edges(source);
        assert!(
            bind_edges
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("settings.Database")),
            "Expected CONFIG_BIND edge to settings.Database, got: {bind_edges:?}"
        );
    }

    #[test]
    fn viper_unmarshal_into_slice_var_emits_no_config_bind() {
        let source = r#"
package main

type ServerConfig struct {
    Host string
}

func loadConfig() {
    var servers []ServerConfig
    viper.UnmarshalKey("servers", &servers)
}
"#;
        let bind_edges = config_bind_edges(source);
        assert!(
            bind_edges.is_empty(),
            "Expected no CONFIG_BIND edges for slice-typed targets, got: {:?}",
            bind_edges
        );
    }

    #[test]
    fn viper_unmarshal_ignores_vars_declared_inside_closures() {
        let source = r#"
package main

type OtherConfig struct {
    Host string
}

func loadConfig() {
    helper := func() {
        cfg := OtherConfig{}
        _ = cfg
    }
    helper()
    viper.Unmarshal(&cfg)
}
"#;
        let bind_edges = config_bind_edges(source);
        assert!(
            bind_edges.is_empty(),
            "Closure-local declarations must not leak into the enclosing scope, got: {:?}",
            bind_edges
        );
    }

    #[test]
    fn viper_unmarshal_with_unresolvable_args_emits_no_config_bind() {
        let source = r#"
package main

func loadConfig() {
    viper.Unmarshal()
    viper.Unmarshal(&undeclared)
    viper.Unmarshal(cfgValue)
    viper.UnmarshalKey("app")
}
"#;
        let bind_edges = config_bind_edges(source);
        assert!(
            bind_edges.is_empty(),
            "Expected no CONFIG_BIND edges for unresolvable arguments, got: {:?}",
            bind_edges
        );
    }

    #[test]
    fn non_viper_call_emits_no_config_bind() {
        let source = r#"
package main

type DatabaseConfig struct {
    Host string
}

func loadConfig() {
    var cfg DatabaseConfig
    json.Unmarshal(data, &cfg)
    yaml.Unmarshal(data, &cfg)
}
"#;
        let bind_edges = config_bind_edges(source);
        assert!(
            bind_edges.is_empty(),
            "Expected no CONFIG_BIND edges for non-viper calls, got: {:?}",
            bind_edges
        );
    }

    // ── Finding 1 ──────────────────────────────────────────────────────────
    // Sibling-block `cfg` declarations must not bleed across scopes.
    // viper.Unmarshal(&cfg) in the FIRST if-block must bind to `AConfig`,
    // not to `BConfig` from the SECOND if-block.
    #[test]
    fn viper_unmarshal_sibling_block_cfg_binds_to_enclosing_scope_type() {
        let source = r#"
package main

type AConfig struct { Host string }
type BConfig struct { Host string }

func f() {
    if a {
        cfg := AConfig{}
        viper.Unmarshal(&cfg)
    }
    if b {
        cfg := BConfig{}
        _ = cfg
    }
}
"#;
        let bind_edges = config_bind_edges(source);
        // Must produce exactly one edge, bound to AConfig.
        assert_eq!(
            bind_edges.len(),
            1,
            "Expected exactly 1 CONFIG_BIND edge, got: {bind_edges:?}"
        );
        assert_eq!(
            bind_edges[0].target_qualname.as_deref(),
            Some("main.AConfig"),
            "Expected edge to main.AConfig (not BConfig from sibling block), got: {bind_edges:?}"
        );
    }

    // ── Finding 2 ──────────────────────────────────────────────────────────
    // A `cfg` shadowed inside a closure must NOT resolve to the outer `cfg`.
    // Acceptable outcomes: bind to `OtherConfig` (if closure locals are tracked)
    // or emit no edge — but never bind to the outer `DatabaseConfig`.
    #[test]
    fn viper_unmarshal_closure_shadow_does_not_bind_to_outer_type() {
        let source = r#"
package main
type DatabaseConfig struct{ Host string }
type OtherConfig struct{ Host string }
func f() {
    var cfg DatabaseConfig
    helper := func() {
        cfg := OtherConfig{}
        viper.Unmarshal(&cfg)
    }
    helper()
}
"#;
        let bind_edges = config_bind_edges(source);
        // Must NOT bind to the outer DatabaseConfig.
        assert!(
            !bind_edges
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("main.DatabaseConfig")),
            "Closure-shadowed cfg must not bind to outer DatabaseConfig, got: {bind_edges:?}"
        );
    }

    // ── Finding 3 ──────────────────────────────────────────────────────────
    // A `comment` node before the struct argument must not shift the positional
    // index so that the edge is silently dropped.
    #[test]
    fn viper_unmarshal_comment_before_arg_does_not_drop_edge() {
        // tree-sitter represents inline block comments as named children of
        // argument_list; we feed the raw Go source through the extractor to
        // exercise the real parser.
        let source = r#"
package main

type AppConfig struct { Debug bool }

func loadConfig() {
    var appCfg AppConfig
    viper.Unmarshal(/* c */ &appCfg)
    viper.UnmarshalKey("app", /* c */ &appCfg)
}
"#;
        let bind_edges = config_bind_edges(source);
        // Both calls should bind to main.AppConfig.
        assert!(
            bind_edges
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("main.AppConfig")),
            "Expected at least one CONFIG_BIND edge to main.AppConfig despite inline comment, got: {bind_edges:?}"
        );
        assert_eq!(
            bind_edges
                .iter()
                .filter(|e| e.target_qualname.as_deref() == Some("main.AppConfig"))
                .count(),
            2,
            "Expected two CONFIG_BIND edges to main.AppConfig (Unmarshal + UnmarshalKey), got: {bind_edges:?}"
        );
    }
}
