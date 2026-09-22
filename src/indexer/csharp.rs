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
use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::rc::Rc;
use tree_sitter::{Node, Parser};

#[derive(Clone)]
struct Context {
    module: String,
    namespace_stack: Vec<String>,
    type_stack: Vec<String>,
    fn_depth: usize,
    current_scope: String,
    route_prefix: Option<String>,
    route_groups: HashMap<String, String>,
    grpc_service: Option<String>,
    /// Locally-bound gRPC client variable names -> `(service, prefix)`,
    /// where `prefix` is whatever qualifying namespace/alias text preceded
    /// the `{Service}.{Service}Client` type in its construction (e.g.
    /// `DsDeploy` in `new DsDeploy.DeployerService.DeployerServiceClient(channel)`)
    /// — mirrors `grpc_service_from_bases`'s `(service, prefix)` shape on
    /// the impl side. `None` when the client type carries no further
    /// qualification beyond the mandatory `{Service}.{Service}Client`
    /// self-reference. Also carries class-level fields/properties of the
    /// *directly enclosing* type (see `collect_class_level_grpc_client_fields`,
    /// merged in `handle_type`), so a bare `Client.Method()` or
    /// `this.Client.Method()` call from within that same type resolves too.
    /// See `collect_grpc_clients_inner` / `grpc_client_from_object_creation`
    /// / `split_client_service_and_prefix`.
    grpc_clients: HashMap<String, (String, Option<String>)>,
    /// Cross-file fallback for a gRPC-client-typed field/property accessed
    /// through a receiver whose own declaring type isn't visible from this
    /// file (`scope.Client.Deploy(...)`, where `scope`'s type — and its
    /// `Client` field — are declared in a different file entirely). See
    /// `GrpcClientFieldRegistry`'s doc.
    grpc_client_fields: GrpcClientFieldRegistry,
    /// Candidate protobuf package names for `grpc_service`, derived from the
    /// impl class's base-list entry (e.g. `DsDeploy.DeployerService.Base`)
    /// plus this file's `using` directives — see
    /// `grpc_package_candidates_from_prefix`. Deliberately *not* derived
    /// from `namespace_stack`: the impl class's own CLR namespace is chosen
    /// for the implementation's code organization and has no reliable
    /// relationship to the proto package it implements (that mismatch was
    /// the bug this field exists to fix). Set alongside `grpc_service` in
    /// `handle_type` and consumed only by `grpc_impl_edge`.
    grpc_package_candidates: Vec<String>,
    /// Types of locally-bound names (parameters + typed/`var` local
    /// declarations) within the *current* method/constructor body only —
    /// see `infer_local_types`. Reset fresh on every method/constructor
    /// entry; never merged across methods. See
    /// `python::infer_receiver_type` for the mechanism this mirrors.
    local_types: Rc<HashMap<String, LocalType>>,
    /// Type-annotated fields and properties of the *directly* enclosing
    /// type, read once when entering its body — see
    /// `collect_class_level_attr_types`. Used only to resolve a single-hop
    /// `this.field.Method()` receiver.
    class_attr_types: Rc<HashMap<String, LocalType>>,
    /// The directly enclosing type's own base class, if its `base_list`
    /// names one and it's resolvable (see `handle_type`) — used only to
    /// resolve a `base.Method()` receiver. `Other` when the type has no
    /// base class, only implements interfaces, or the first base-list
    /// entry isn't cheaply classifiable.
    base_type: LocalType,
    /// This file's `using` directives, collected once in `extract()` before
    /// the main walk — see `ImportContext` / `collect_import_context`. Set
    /// once and inherited unchanged through every `ctx.clone()` (unlike
    /// `local_types`/`class_attr_types`, this never changes per-scope).
    imports: Rc<ImportContext>,
    /// Every extension method (`public static X Foo(this T x, ...)`) seen
    /// so far in *any* file processed by this `CSharpExtractor` instance
    /// during the current reindex — see `ExtensionRegistry` and
    /// `record_extension_method`. Shared (same underlying map, not a
    /// per-file copy) via `Rc<RefCell<_>>` so a declaration recorded while
    /// walking one file is visible to call sites in a later file — the only
    /// way a single-file extractor can name a cross-file extension method's
    /// real declaring class (see `extension_method_candidates`'s doc for why
    /// that's unavoidable). Grows monotonically; never pruned or reset
    /// between files, so a full cold reindex ends with every extension
    /// method the repo declares, in file-processing order. A call site
    /// whose extension method hasn't been visited *yet* this run simply
    /// gets no candidate from this source — see the ponytail note on
    /// `extension_method_candidates`.
    extension_registry: ExtensionRegistry,
}

/// One extension method declaration, as recorded by `record_extension_method`
/// into `Context::extension_registry` — see that field's doc for why this
/// state is accumulated across files instead of derived per-call.
#[derive(Debug, Clone)]
struct ExtensionMethodEntry {
    /// The method's own fully-qualified qualname (declaring namespace +
    /// class + method name) — exactly the string `SymbolInput::qualname`
    /// carries for this same declaration, so it's an exact match for
    /// whatever `Db::insert_edges`'s exact-qualname lookup sees once this
    /// file has been indexed.
    qualname: String,
    /// The declaring class's enclosing namespace (`ctx.namespace_stack`
    /// joined), i.e. what a calling file's `using` directive must name for
    /// this extension method to be in scope there — see
    /// `namespace_in_scope`.
    namespace: String,
    /// The extended (`this`) parameter's type name, when it classifies as a
    /// concrete non-builtin type (see `classify_annotation`) — `None` for a
    /// generic type parameter, builtin, or otherwise unclassifiable shape,
    /// meaning "can't rule this entry out by type" rather than "matches
    /// anything for certain".
    receiver_type: Option<String>,
}

/// Keyed by bare method name (e.g. "ToDomain") -> every extension method
/// declaration seen under that name so far this run.
type ExtensionRegistry = Rc<RefCell<HashMap<String, Vec<ExtensionMethodEntry>>>>;

/// Keyed by bare field/property name (e.g. "Client") -> every
/// `(service, prefix)` a gRPC-client-typed field or property declared under
/// that name has been seen with so far this run, across every file — see
/// `collect_class_level_grpc_client_fields`. Mirrors `ExtensionRegistry`'s
/// cross-file, name-keyed, order-dependent design (same ponytail caveat:
/// a call site in a file processed before the field's declaring file gets
/// no candidate from this source) for the identical reason: a generated
/// gRPC client is very often exposed through a same-named field on a small,
/// repeated test-fixture shape (dpb's own corpus: eight distinct generated
/// clients, all exposed as a field literally named `Client`, in a different
/// file than every one of their call sites), so this exists purely to
/// bridge that gap. A lookup here (`grpc_service_from_client_binding`) fans
/// out over every candidate rather than picking one — the receiver's own
/// declaring type is invisible from here, so there's no way to disambiguate
/// — exactly the same "a wrong candidate simply never matches a real route
/// downstream" tolerance `grpc_impl_edge`/`grpc_call_edge` already rely on
/// for candidate *packages*. Every entry here already passed
/// `split_client_service_and_prefix`'s mandatory self-reference check
/// before being admitted, so this can only ever fan out over genuine
/// generated-code candidates, never arbitrary `...Client`-suffixed types.
type GrpcClientFieldRegistry = Rc<RefCell<HashMap<String, Vec<(String, Option<String>)>>>>;

/// Locally-inferred type of a name bound within a single method/constructor
/// body (or a class-level field/property/parameter-property). Deliberately
/// coarse — see `python::LocalType` for the shape this mirrors; everything
/// that isn't a confident, non-builtin type name collapses to `Other`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LocalType {
    /// Inferred (via declared type or `new T()` construction) to be this
    /// non-builtin type name.
    Known(String),
    /// Builtin type, `var` without a `new T()` initializer, loop/catch
    /// target of unresolvable type, or anything else not explicitly
    /// recognized. A name landing here (rather than simply absent from the
    /// map) still gates resolution: it means "we looked, and it's not a
    /// usable type" as opposed to "we never looked".
    Other,
}

pub struct CSharpExtractor {
    parser: Parser,
    /// Accumulates across every file this extractor instance processes —
    /// see `Context::extension_registry`'s doc.
    extension_registry: ExtensionRegistry,
    /// Accumulates across every file this extractor instance processes —
    /// see `GrpcClientFieldRegistry`'s doc.
    grpc_client_fields: GrpcClientFieldRegistry,
}

impl CSharpExtractor {
    pub fn new() -> Result<Self> {
        let mut parser = Parser::new();
        let language = tree_sitter_c_sharp::LANGUAGE;
        parser.set_language(&language.into())?;
        Ok(Self {
            parser,
            extension_registry: Rc::new(RefCell::new(HashMap::new())),
            grpc_client_fields: Rc::new(RefCell::new(HashMap::new())),
        })
    }
}

impl crate::indexer::extract::LanguageExtractor for CSharpExtractor {
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
        let ctx = Context {
            module: module_name.to_string(),
            namespace_stack: Vec::new(),
            type_stack: Vec::new(),
            fn_depth: 0,
            current_scope: module_name.to_string(),
            route_prefix: None,
            route_groups: HashMap::new(),
            grpc_service: None,
            grpc_clients: HashMap::new(),
            grpc_client_fields: Rc::clone(&self.grpc_client_fields),
            grpc_package_candidates: Vec::new(),
            // ponytail: unlike Python/TypeScript, there's no meaningful
            // module-top-level scope in C# (locals only ever live inside a
            // method/constructor body), so this starts and stays empty
            // outside of `handle_method`/`handle_constructor`.
            local_types: Rc::new(HashMap::new()),
            class_attr_types: Rc::new(HashMap::new()),
            base_type: LocalType::Other,
            imports: Rc::new(collect_import_context(root, source)),
            extension_registry: Rc::clone(&self.extension_registry),
        };
        if root.kind() == "compilation_unit" {
            walk_compilation_unit(root, &ctx, source, &mut output);
        } else {
            walk_node(root, &ctx, source, &mut output);
        }
        Ok(output)
    }
}

pub fn module_name_from_rel_path(rel_path: &str) -> String {
    let path = Path::new(rel_path);
    let mut parts: Vec<String> = path
        .components()
        .filter_map(|comp| comp.as_os_str().to_str().map(|s| s.to_string()))
        .collect();
    if parts.is_empty() {
        return "module".to_string();
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
        "module".to_string()
    } else {
        parts.join("/")
    }
}

fn walk_compilation_unit(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut file_ns_name = None;
    let mut file_ns_span = None;
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "file_scoped_namespace_declaration" {
            file_ns_name = namespace_name(child, source);
            file_ns_span = Some(span(child));
            break;
        }
    }

    let mut next_ctx = ctx.clone();
    if let Some(name) = file_ns_name {
        let parts = namespace_parts(&name);
        let qualname = parts.join(".");
        if !qualname.is_empty() {
            let name = parts.last().cloned().unwrap_or_else(|| qualname.clone());
            let span = file_ns_span.unwrap_or_else(|| span(node));
            output.symbols.push(SymbolInput {
                kind: "namespace".to_string(),
                name,
                qualname: qualname.clone(),
                start_line: span.0,
                start_col: span.1,
                end_line: span.2,
                end_col: span.3,
                start_byte: span.4,
                end_byte: span.5,
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
            next_ctx.namespace_stack = parts;
            next_ctx.current_scope = qualname;
        }
    }
    next_ctx.route_groups = collect_global_route_groups(node, source);
    next_ctx.grpc_clients = collect_global_grpc_clients(node, source);

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "file_scoped_namespace_declaration" {
            continue;
        }
        walk_node(child, &next_ctx, source, output);
    }
}

fn walk_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if node.kind() == "invocation_expression" || node.kind() == "object_creation_expression" {
        handle_call(node, ctx, source, output);
    }
    // Configuration["KEY"] — element_access_expression
    if node.kind() == "element_access_expression"
        && let Some(edge) = config_indexer_read_edge(node, ctx, source)
    {
        output.edges.push(edge);
    }
    if is_nested_function_node(node.kind()) {
        return;
    }
    match node.kind() {
        "namespace_declaration" => {
            handle_namespace(node, ctx, source, output);
            return;
        }
        "class_declaration" => {
            handle_type(node, ctx, source, output, "class", TypeKind::Class);
            return;
        }
        "struct_declaration" => {
            handle_type(node, ctx, source, output, "struct", TypeKind::Struct);
            return;
        }
        "interface_declaration" => {
            handle_type(node, ctx, source, output, "interface", TypeKind::Interface);
            return;
        }
        "record_declaration" => {
            handle_type(node, ctx, source, output, "record", TypeKind::Record);
            return;
        }
        "enum_declaration" => {
            handle_type(node, ctx, source, output, "enum", TypeKind::Enum);
            return;
        }
        "method_declaration" => {
            handle_method(node, ctx, source, output);
            return;
        }
        "constructor_declaration" => {
            handle_constructor(node, ctx, source, output);
            return;
        }
        "property_declaration" => {
            handle_property(node, ctx, source, output);
            return;
        }
        "field_declaration" => {
            handle_field(node, ctx, source, output);
            return;
        }
        "using_directive" => {
            handle_using(node, ctx, source, output);
            return;
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

fn handle_namespace(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name) = namespace_name(node, source) else {
        return;
    };
    let parts = namespace_parts(&name);
    if parts.is_empty() {
        return;
    }
    let mut next_ctx = ctx.clone();
    let mut full_parts = next_ctx.namespace_stack.clone();
    full_parts.extend(parts.clone());
    let qualname = full_parts.join(".");
    let name = parts.last().cloned().unwrap_or_else(|| qualname.clone());
    let span = span(node);
    output.symbols.push(SymbolInput {
        kind: "namespace".to_string(),
        name,
        qualname: qualname.clone(),
        start_line: span.0,
        start_col: span.1,
        end_line: span.2,
        end_col: span.3,
        start_byte: span.4,
        end_byte: span.5,
        signature: None,
        docstring: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(container_qualname(ctx)),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    next_ctx.namespace_stack = full_parts;
    next_ctx.current_scope = qualname;
    if let Some(body) = node.child_by_field_name("body") {
        walk_declaration_list(body, &next_ctx, source, output);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TypeKind {
    Class,
    Struct,
    Interface,
    Record,
    Enum,
}

fn handle_type(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
    kind: &str,
    type_kind: TypeKind,
) {
    if ctx.fn_depth > 0 {
        return;
    }
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(ctx, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = type_signature(node, source);
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
        source_qualname: Some(container_qualname(ctx)),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    if type_kind != TypeKind::Enum {
        handle_base_list(node, &qualname, source, output, type_kind);
    }

    let grpc_service_info = grpc_service_from_bases(node, source);
    let grpc_package_candidates = grpc_service_info
        .as_ref()
        .map(|(_, prefix)| grpc_package_candidates_from_prefix(prefix.as_deref(), ctx))
        .unwrap_or_default();
    let class_prefix = route_prefix_from_attributes(node, source);
    let combined_prefix =
        combine_route_prefix(ctx.route_prefix.as_deref(), class_prefix.as_deref());
    let mut next_ctx = ctx.clone();
    next_ctx.type_stack.push(name);
    next_ctx.current_scope = qualname;
    next_ctx.route_prefix = combined_prefix;
    next_ctx.grpc_service = grpc_service_info.map(|(service, _)| service);
    next_ctx.grpc_package_candidates = grpc_package_candidates;
    next_ctx.base_type = resolvable_base_type(node, source, type_kind);
    if let Some(body) = node.child_by_field_name("body") {
        next_ctx.class_attr_types = Rc::new(collect_class_level_attr_types(body, source));
        let grpc_fields = collect_class_level_grpc_client_fields(body, source);
        if !grpc_fields.is_empty() {
            let mut clients = next_ctx.grpc_clients.clone();
            {
                let mut registry = next_ctx.grpc_client_fields.borrow_mut();
                for (field_name, service_and_prefix) in grpc_fields {
                    let entries = registry.entry(field_name.clone()).or_default();
                    if !entries.contains(&service_and_prefix) {
                        entries.push(service_and_prefix.clone());
                    }
                    clients.entry(field_name).or_insert(service_and_prefix);
                }
            }
            next_ctx.grpc_clients = clients;
        }
        walk_declaration_list(body, &next_ctx, source, output);
    }
}

/// The directly enclosing type's own base *class* (not an interface), if
/// resolvable — used to gate a `base.Method()` receiver. Reuses the same
/// first-base-entry-is-the-class convention `handle_base_list` already
/// applies when choosing EXTENDS vs. IMPLEMENTS.
fn resolvable_base_type(node: Node<'_>, source: &str, type_kind: TypeKind) -> LocalType {
    if !matches!(type_kind, TypeKind::Class | TypeKind::Record) {
        return LocalType::Other;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "base_list" {
            continue;
        }
        let bases = base_list_types(child, source);
        let Some(first) = bases.first() else {
            return LocalType::Other;
        };
        if is_likely_interface_name(first) {
            return LocalType::Other;
        }
        return classify_annotation(first);
    }
    LocalType::Other
}

fn handle_method(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(ctx, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = method_signature(node, source);
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
        source_qualname: Some(container_qualname(ctx)),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
    for edge in grpc_impl_edge(node, ctx, source, &name) {
        output.edges.push(edge);
    }
    for edge in route_edges_from_method_attributes(node, ctx, source, &qualname) {
        output.edges.push(edge);
    }
    record_extension_method(node, ctx, source, &name, &qualname);
    if let Some(body) = node.child_by_field_name("body") {
        let mut next_ctx = ctx.clone();
        next_ctx.fn_depth += 1;
        next_ctx.current_scope = qualname;
        next_ctx.route_groups = collect_route_groups(body, source);
        let mut grpc_clients = ctx.grpc_clients.clone();
        grpc_clients.extend(collect_grpc_clients(body, source));
        next_ctx.grpc_clients = grpc_clients;
        next_ctx.local_types = Rc::new(infer_local_types(node, source));
        walk_node(body, &next_ctx, source, output);
    }
}

fn handle_constructor(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let qualname = build_qualname(ctx, ".ctor");
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = method_signature(node, source);
    output.symbols.push(SymbolInput {
        kind: "method".to_string(),
        name: ".ctor".to_string(),
        qualname: qualname.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature: signature.clone(),
        docstring: None,
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(container_qualname(ctx)),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });

    // Detect IOptions<T> constructor injection → CONFIG_BIND edges
    if let Some(ref sig) = signature {
        let class_qualname = container_qualname(ctx);
        for (options_type, wrapper_type) in extract_di_options_types_from_params(sig) {
            let detail = config::build_config_bind_detail(
                &options_type,
                &wrapper_type,
                "constructor_injection",
                "dotnet",
            );
            output.edges.push(EdgeInput {
                kind: config::CONFIG_BIND_KIND.to_string(),
                source_qualname: Some(class_qualname.clone()),
                target_qualname: Some(options_type),
                detail: Some(detail),
                evidence_start_line: Some(start_line),
                evidence_end_line: Some(end_line),
                ..Default::default()
            });
        }
    }

    if let Some(body) = node.child_by_field_name("body") {
        let mut next_ctx = ctx.clone();
        next_ctx.fn_depth += 1;
        next_ctx.current_scope = qualname;
        next_ctx.local_types = Rc::new(infer_local_types(node, source));
        walk_node(body, &next_ctx, source, output);
    }
}

fn handle_property(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(ctx, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    output.symbols.push(SymbolInput {
        kind: "property".to_string(),
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
        source_qualname: Some(container_qualname(ctx)),
        target_qualname: Some(qualname),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
}

fn handle_field(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "variable_declaration" {
            continue;
        }
        handle_variable_declaration(child, ctx, source, output);
    }
}

fn handle_variable_declaration(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "variable_declarator" {
            continue;
        }
        let Some(name_node) = child.child_by_field_name("name") else {
            continue;
        };
        let name = node_text(name_node, source);
        if name.is_empty() {
            continue;
        }
        let qualname = build_qualname(ctx, &name);
        let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(child);
        output.symbols.push(SymbolInput {
            kind: "field".to_string(),
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
            source_qualname: Some(container_qualname(ctx)),
            target_qualname: Some(qualname),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }
}

fn handle_using(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    let mut target = None;
    for child in node.named_children(&mut cursor) {
        if child.kind() == "type" {
            let name = node_text(child, source);
            if !name.is_empty() {
                target = Some(name);
                break;
            }
        }
    }
    if target.is_none() {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            match child.kind() {
                "qualified_name" | "identifier" | "generic_name" | "alias_qualified_name" => {
                    let name = node_text(child, source);
                    if !name.is_empty() {
                        target = Some(name);
                        break;
                    }
                }
                _ => {}
            }
        }
    }
    let Some(target) = target else {
        return;
    };
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    output.edges.push(EdgeInput {
        kind: "IMPORTS".to_string(),
        source_qualname: Some(base_qualname(ctx)),
        target_qualname: Some(target),
        detail: None,
        evidence_snippet: snippet,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    });
}

fn handle_call(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    for edge in http_route_edges(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = http_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    for edge in grpc_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = channel_publish_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = channel_subscribe_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = config_read_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    if let Some(edge) = config_bind_call_edge(node, ctx, source) {
        output.edges.push(edge);
    }
    let Some(target_node) = call_target_node(node) else {
        return;
    };
    let raw = node_text(target_node, source);
    if raw.is_empty() {
        return;
    }
    let receiver_type = infer_receiver_type(target_node, source, ctx);
    // Import-aware qualification only makes sense for a call whose receiver
    // isn't already gated by receiver-type inference (a tracked local/field
    // is never a type name) — see `import_qualified_candidates`'s doc.
    // `raw` is collapsed here too (same as `resolve_call_target` does
    // internally) so a multi-line `UniqueName\n    .Create()` chain feeds
    // this tier the same shape the single-line form would.
    let type_call_candidates = if receiver_type == ReceiverType::NotTracked {
        let collapsed = collapse_call_target_whitespace(&raw);
        type_prefixed_receiver_and_suffix(&collapsed)
            .map(|(receiver, suffix)| import_qualified_candidates(receiver, suffix, ctx))
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    // Extension-method candidates are attempted for *any* receiver shape —
    // unlike the static-call tier above, an extension call's receiver is
    // routinely a tracked local/field (`_connection.EnsureOpenAsync()`) or
    // an unresolved one (`row.ToDomain()`), never a type name, so gating on
    // `NotTracked` would miss the common case. Only a genuine
    // `receiver.Method()` shape qualifies — a bare `Helper()` call has no
    // receiver to extend and always resolves through the ordinary
    // exact/container tier first regardless. See
    // `extension_method_candidates`'s doc for why this needs its own
    // (cross-file, accumulated) evidence source rather than reusing
    // `import_qualified_candidates`.
    let extension_candidates = call_target_parts(target_node, source)
        .filter(|parts| parts.receiver.is_some())
        .map(|parts| extension_method_candidates(&parts.name, &receiver_type, ctx))
        .unwrap_or_default();
    // Union rather than replace: on the rare chance both tiers produce a
    // (necessarily different) candidate, let `resolve_import_candidate`'s
    // own ambiguity guard see both and refuse rather than silently
    // preferring one.
    let mut import_candidates = type_call_candidates;
    for candidate in extension_candidates {
        if !import_candidates.contains(&candidate) {
            import_candidates.push(candidate);
        }
    }
    let target = resolve_call_target(&raw, ctx);
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
        import_candidates,
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    });
}

fn config_read_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let target_node = call_target_node(node)?;
    let ct = call_target_parts(target_node, source)?;
    let receiver = ct.receiver.as_deref().unwrap_or("");

    // Environment.GetEnvironmentVariable("KEY")
    if ct.name == "GetEnvironmentVariable"
        && (receiver == "Environment" || receiver.ends_with(".Environment"))
    {
        let args = call_arguments(node);
        let key = args
            .first()
            .and_then(|a| extract_string_literal(*a, source))?;
        let env_uri = config::normalize_env_var_name(&key)?;
        let detail = config::build_config_read_detail("env", &env_uri, &key, "dotnet");
        let (start_line, _, end_line, _, _, _) = span(node);
        return Some(EdgeInput {
            kind: config::CONFIG_READ_KIND.to_string(),
            source_qualname: Some(ctx.current_scope.clone()),
            target_qualname: Some(env_uri),
            detail: Some(detail),
            evidence_start_line: Some(start_line),
            evidence_end_line: Some(end_line),
            ..Default::default()
        });
    }

    // IConfiguration.GetValue<T>("KEY") or GetValue("KEY")
    // Also: .BindConfiguration("Section") for options pattern
    if ct.name == "GetValue"
        || ct.name == "GetSection"
        || ct.name == "GetConnectionString"
        || ct.name == "BindConfiguration"
    {
        let args = call_arguments(node);
        let key = args
            .first()
            .and_then(|a| extract_string_literal(*a, source))?;
        let env_uri = config::normalize_env_var_name(&key)?;
        let detail = config::build_config_read_detail("config", &env_uri, &key, "dotnet-config");
        let (start_line, _, end_line, _, _, _) = span(node);
        return Some(EdgeInput {
            kind: config::CONFIG_READ_KIND.to_string(),
            source_qualname: Some(ctx.current_scope.clone()),
            target_qualname: Some(env_uri),
            detail: Some(detail),
            evidence_start_line: Some(start_line),
            evidence_end_line: Some(end_line),
            ..Default::default()
        });
    }

    None
}

/// Detect Configure<T>(), AddOptions<T>(), GetRequiredService<IOptions<T>>() calls.
fn config_bind_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let target_node = call_target_node(node)?;
    let ct = call_target_parts(target_node, source)?;

    // services.Configure<T>(...) or services.AddOptions<T>()
    let method_base = ct.name.split('<').next().unwrap_or(&ct.name);
    let is_config_method = matches!(method_base, "Configure" | "AddOptions");

    let options_type = if is_config_method {
        extract_generic_type_arg(&ct.name)
    } else if method_base == "GetRequiredService" || method_base == "GetService" {
        // GetRequiredService<IOptions<T>>() → unwrap IOptions
        let inner = extract_generic_type_arg(&ct.name)?;
        extract_options_type(&inner).or(Some(inner))
    } else {
        None
    }?;

    let detail =
        config::build_config_bind_detail(&options_type, method_base, "configure_call", "dotnet");
    let (start_line, _, end_line, _, _, _) = span(node);
    Some(EdgeInput {
        kind: config::CONFIG_BIND_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(options_type),
        detail: Some(detail),
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    })
}

/// Detect Configuration["KEY"] or ConfigurationManager.AppSettings["KEY"] indexer access.
fn config_indexer_read_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    // element_access_expression has "expression" (the object) and "argument_list" (the bracket args)
    let expr_node = node.child_by_field_name("expression")?;
    let receiver_text = node_text(expr_node, source);
    let receiver_lower = receiver_text.to_ascii_lowercase();

    // Match configuration["key"], _configuration["key"], Configuration["key"]
    let is_config = receiver_lower.ends_with("configuration")
        || receiver_lower.ends_with("appsettings")
        || receiver_lower.contains("configurationmanager.appsettings")
        || receiver_lower.contains("configurationmanager.connectionstrings");

    if !is_config {
        return None;
    }

    // Extract the string key from the bracket list
    let arg_list = node.child_by_field_name("argument_list")?;
    let mut cursor = arg_list.walk();
    for child in arg_list.named_children(&mut cursor) {
        if let Some(key) = extract_string_literal(child, source) {
            let env_uri = config::normalize_env_var_name(&key)?;
            let detail =
                config::build_config_read_detail("config", &env_uri, &key, "dotnet-config");
            let (start_line, _, end_line, _, _, _) = span(node);
            return Some(EdgeInput {
                kind: config::CONFIG_READ_KIND.to_string(),
                source_qualname: Some(ctx.current_scope.clone()),
                target_qualname: Some(env_uri),
                detail: Some(detail),
                evidence_start_line: Some(start_line),
                evidence_end_line: Some(end_line),
                ..Default::default()
            });
        }
    }
    None
}

#[derive(Clone)]
struct AttributeInfo<'a> {
    name: String,
    args: Vec<Node<'a>>,
    node: Node<'a>,
}

struct CallTarget {
    receiver: Option<String>,
    name: String,
    full: String,
}

fn route_prefix_from_attributes(node: Node<'_>, source: &str) -> Option<String> {
    for attr in attributes_for_node(node, source) {
        let name = normalize_attribute_name(&attr.name);
        if (name == "Route" || name == "RoutePrefix")
            && let Some(template) = attribute_first_string_arg(&attr, source)
        {
            return Some(template);
        }
    }
    None
}

fn combine_route_prefix(prefix: Option<&str>, next: Option<&str>) -> Option<String> {
    match (prefix, next) {
        (Some(prefix), Some(next)) => Some(http::join_paths(prefix, next)),
        (Some(prefix), None) => Some(prefix.to_string()),
        (None, Some(next)) => Some(next.to_string()),
        (None, None) => None,
    }
}

fn route_edges_from_method_attributes(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    handler: &str,
) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    let attrs = attributes_for_node(node, source);
    if attrs.is_empty() {
        return edges;
    }
    let mut route_template = None;
    let mut route_node = None;
    let mut saw_route_attr = false;
    let mut method_edges: Vec<(String, Option<String>, Node<'_>)> = Vec::new();
    for attr in attrs {
        let name = normalize_attribute_name(&attr.name);
        if name == "Route" || name == "RoutePrefix" {
            saw_route_attr = true;
            if route_template.is_none() {
                route_template = attribute_first_string_arg(&attr, source);
                route_node = Some(attr.node);
            }
            continue;
        }
        if name == "AcceptVerbs" {
            for method in attribute_string_list(&attr, source) {
                if let Some(method) = http::normalize_method(&method) {
                    method_edges.push((method, None, attr.node));
                }
            }
            continue;
        }
        if let Some(method) = http_method_from_attribute(&name) {
            let path = attribute_first_string_arg(&attr, source);
            method_edges.push((method, path, attr.node));
        }
    }

    let prefix = ctx.route_prefix.as_deref();
    if !method_edges.is_empty() {
        for (method, path, node) in method_edges {
            let raw_path = combine_route(prefix, path.as_deref().or(route_template.as_deref()));
            let Some(raw_path) = raw_path else {
                continue;
            };
            if let Some(edge) =
                build_route_edge(handler, &method, &raw_path, "aspnet", node, source)
            {
                edges.push(edge);
            }
        }
        return edges;
    }

    if saw_route_attr {
        let raw_path = combine_route(prefix, route_template.as_deref());
        let Some(raw_path) = raw_path else {
            return edges;
        };
        if let Some(node) = route_node
            && let Some(edge) =
                build_route_edge(handler, http::HTTP_ANY, &raw_path, "aspnet", node, source)
        {
            edges.push(edge);
        }
    }
    edges
}

fn combine_route(prefix: Option<&str>, path: Option<&str>) -> Option<String> {
    match (prefix, path) {
        (Some(prefix), Some(path)) => Some(http::join_paths(prefix, path)),
        (Some(prefix), None) => Some(http::join_paths(prefix, "")),
        (None, Some(path)) => Some(path.to_string()),
        (None, None) => None,
    }
}

fn attributes_for_node<'a>(node: Node<'a>, source: &str) -> Vec<AttributeInfo<'a>> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "attribute_list" {
            continue;
        }
        let mut list_cursor = child.walk();
        for attr in child.named_children(&mut list_cursor) {
            if attr.kind() != "attribute" {
                continue;
            }
            let Some(name_node) = attr.child_by_field_name("name") else {
                continue;
            };
            let raw_name = node_text(name_node, source);
            if raw_name.is_empty() {
                continue;
            }
            let args = attribute_argument_exprs(attr);
            out.push(AttributeInfo {
                name: raw_name,
                args,
                node: attr,
            });
        }
    }
    out
}

fn attribute_argument_exprs(node: Node<'_>) -> Vec<Node<'_>> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "attribute_argument_list" {
            continue;
        }
        let mut arg_cursor = child.walk();
        for arg in child.named_children(&mut arg_cursor) {
            if arg.kind() != "attribute_argument" {
                continue;
            }
            if let Some(expr) = attribute_argument_expr(arg) {
                out.push(expr);
            }
        }
    }
    out
}

fn attribute_argument_expr(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    let mut expr = None;
    for child in node.named_children(&mut cursor) {
        expr = Some(child);
    }
    expr
}

fn attribute_first_string_arg(attr: &AttributeInfo<'_>, source: &str) -> Option<String> {
    for arg in &attr.args {
        if let Some(value) = extract_string_literal(*arg, source) {
            return Some(value);
        }
    }
    None
}

fn attribute_string_list(attr: &AttributeInfo<'_>, source: &str) -> Vec<String> {
    let mut out = Vec::new();
    for arg in &attr.args {
        out.extend(extract_string_list(*arg, source));
    }
    out
}

fn normalize_attribute_name(raw: &str) -> String {
    let name = raw.rsplit('.').next().unwrap_or(raw).to_string();
    name.strip_suffix("Attribute").unwrap_or(&name).to_string()
}

fn http_method_from_attribute(name: &str) -> Option<String> {
    let rest = name.strip_prefix("Http")?;
    if rest.is_empty() {
        return None;
    }
    http::normalize_method(rest)
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
    edges.extend(route_edges_from_map_call(node, ctx, source));
    edges
}

fn route_edges_from_map_call(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    let mut edges = Vec::new();
    if node.kind() != "invocation_expression" {
        return edges;
    }
    let Some(target_node) = node.child_by_field_name("function") else {
        return edges;
    };
    let Some(target) = call_target_parts(target_node, source) else {
        return edges;
    };
    if !target.name.starts_with("Map") {
        return edges;
    }
    let args = call_arguments(node);
    let Some(raw_path) = args
        .first()
        .and_then(|arg| extract_string_literal(*arg, source))
    else {
        return edges;
    };
    let group_prefix = map_group_prefix_from_receiver(target_node, ctx, source);
    let prefix = combine_route_prefix(ctx.route_prefix.as_deref(), group_prefix.as_deref());
    let raw_path = combine_route(prefix.as_deref(), Some(&raw_path)).unwrap_or(raw_path);
    let mut methods = Vec::new();
    let handler = if target.name == "MapMethods" {
        let list = args.get(1).map(|arg| extract_string_list(*arg, source));
        if let Some(list) = list {
            methods.extend(
                list.into_iter()
                    .filter_map(|method| http::normalize_method(&method)),
            );
        }
        if methods.is_empty() {
            methods.push(http::HTTP_ANY.to_string());
        }
        args.get(2)
            .and_then(|arg| handler_name_from_expr(*arg, ctx, source))
    } else if target.name == "Map" {
        methods.push(http::HTTP_ANY.to_string());
        args.get(1)
            .and_then(|arg| handler_name_from_expr(*arg, ctx, source))
    } else {
        let Some(method) = target.name.strip_prefix("Map") else {
            return edges;
        };
        let Some(method) = http::normalize_method(method) else {
            return edges;
        };
        methods.push(method);
        args.get(1)
            .and_then(|arg| handler_name_from_expr(*arg, ctx, source))
    };
    let handler = handler.unwrap_or_else(|| ctx.current_scope.clone());
    for method in methods {
        if let Some(edge) = build_route_edge(&handler, &method, &raw_path, "aspnet", node, source) {
            edges.push(edge);
        }
    }
    edges
}

fn map_group_prefix_from_receiver(node: Node<'_>, ctx: &Context, source: &str) -> Option<String> {
    if node.kind() != "member_access_expression" {
        return None;
    }
    let receiver = node.child_by_field_name("expression")?;
    if receiver.kind() == "invocation_expression" {
        return map_group_prefix_from_invocation(receiver, source);
    }
    let receiver_text = node_text(receiver, source);
    if receiver_text.is_empty() {
        return None;
    }
    if let Some(prefix) = ctx.route_groups.get(&receiver_text) {
        return Some(prefix.clone());
    }
    if let Some(last) = receiver_text.rsplit('.').next()
        && let Some(prefix) = ctx.route_groups.get(last)
    {
        return Some(prefix.clone());
    }
    None
}

fn map_group_prefix_from_invocation(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() != "invocation_expression" {
        return None;
    }
    let function = node.child_by_field_name("function")?;
    let target = call_target_parts(function, source)?;
    if target.name != "MapGroup" {
        return None;
    }
    let args = call_arguments(node);
    let path = args
        .first()
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let mut prefix = path;
    if function.kind() == "member_access_expression"
        && let Some(receiver) = function.child_by_field_name("expression")
        && let Some(parent) = map_group_prefix_from_invocation(receiver, source)
    {
        prefix = http::join_paths(&parent, &prefix);
    }
    Some(prefix)
}

fn collect_global_route_groups(node: Node<'_>, source: &str) -> HashMap<String, String> {
    let mut groups = HashMap::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "global_statement" {
            continue;
        }
        collect_route_groups_inner(child, source, &mut groups);
    }
    groups
}

fn collect_route_groups(node: Node<'_>, source: &str) -> HashMap<String, String> {
    let mut groups = HashMap::new();
    collect_route_groups_inner(node, source, &mut groups);
    groups
}

fn collect_global_grpc_clients(
    node: Node<'_>,
    source: &str,
) -> HashMap<String, (String, Option<String>)> {
    let mut clients = HashMap::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "global_statement" {
            continue;
        }
        collect_grpc_clients_inner(child, source, &mut clients);
    }
    clients
}

fn collect_grpc_clients(node: Node<'_>, source: &str) -> HashMap<String, (String, Option<String>)> {
    let mut clients = HashMap::new();
    collect_grpc_clients_inner(node, source, &mut clients);
    clients
}

fn collect_route_groups_inner(node: Node<'_>, source: &str, groups: &mut HashMap<String, String>) {
    match node.kind() {
        "method_declaration"
        | "local_function_statement"
        | "class_declaration"
        | "struct_declaration"
        | "record_declaration"
        | "interface_declaration"
        | "enum_declaration" => {
            return;
        }
        _ => {}
    }
    if node.kind() == "variable_declarator"
        && let Some((name, prefix)) = route_group_from_declarator(node, source)
    {
        groups.insert(name, prefix);
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_route_groups_inner(child, source, groups);
    }
}

fn collect_grpc_clients_inner(
    node: Node<'_>,
    source: &str,
    clients: &mut HashMap<String, (String, Option<String>)>,
) {
    match node.kind() {
        "method_declaration"
        | "local_function_statement"
        | "class_declaration"
        | "struct_declaration"
        | "record_declaration"
        | "interface_declaration"
        | "enum_declaration" => {
            return;
        }
        // Intercept one level above `variable_declarator` so the
        // declaration's own explicit type (a sibling of the declarator, not
        // a field of it) is in reach — see
        // `collect_grpc_clients_from_declaration`'s doc for why that's
        // needed. Every `variable_declarator` is a child of exactly one
        // `variable_declaration` (local var or field; `foreach`/`catch`/
        // deconstruction bindings use different node shapes entirely — see
        // `collect_statement_bindings`'s identical assumption), so handling
        // it here and stopping is equivalent in coverage to the old
        // bottom-up match on `variable_declarator` directly, just able to
        // see the declared type too.
        "variable_declaration" => {
            collect_grpc_clients_from_declaration(node, source, clients);
            return;
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_grpc_clients_inner(child, source, clients);
    }
}

/// Registers every `variable_declarator` in a single `variable_declaration`
/// (a local variable statement, or — via `collect_class_level_grpc_client_fields`'s
/// reuse of this same function for a field's inner `variable_declaration` —
/// a field) as a gRPC client binding, preferring whatever the initializer
/// itself names (an explicitly typed `new Greeter.GreeterClient(...)`,
/// however deeply nested — e.g. inside a ternary — since that's at least as
/// specific as the declaration's own type, and it's the only signal
/// available at all for a `var` declaration) and falling back to the
/// declaration's own explicit type otherwise.
///
/// That fallback is what makes a C# 9 target-typed `new(channel)`
/// resolvable at all: `implicit_object_creation_expression` has no `type`
/// child of its own (see `grpc_client_from_object_creation`'s doc), so
/// `grpc_client_from_initializer` never finds anything there — the target
/// type only ever exists on the declaration wrapped around it
/// (`TeamService.TeamServiceClient client = new(channel);`). The same
/// fallback also covers a field whose own initializer isn't a construction
/// at all (`public readonly TeamService.TeamServiceClient Client = default!;`,
/// dpb's actual shape — the client is really constructed elsewhere and
/// assigned in via a constructor parameter), since the declared type alone
/// is sufficient evidence once it's passed `split_client_service_and_prefix`.
fn collect_grpc_clients_from_declaration(
    node: Node<'_>,
    source: &str,
    clients: &mut HashMap<String, (String, Option<String>)>,
) {
    let declared = node
        .child_by_field_name("type")
        .filter(|t| t.kind() != "implicit_type")
        .and_then(|t| split_client_service_and_prefix(&node_text(t, source)));
    let mut cursor = node.walk();
    for declarator in node.named_children(&mut cursor) {
        if declarator.kind() != "variable_declarator" {
            continue;
        }
        let Some(name_node) = declarator.child_by_field_name("name") else {
            continue;
        };
        let name = node_text(name_node, source);
        if name.is_empty() {
            continue;
        }
        let from_initializer = declarator
            .child_by_field_name("initializer")
            .and_then(|initializer| grpc_client_from_initializer(initializer, source))
            .or_else(|| grpc_client_from_initializer(declarator, source));
        if let Some(service_and_prefix) = from_initializer.or_else(|| declared.clone()) {
            clients.insert(name, service_and_prefix);
        }
    }
}

fn route_group_from_declarator(node: Node<'_>, source: &str) -> Option<(String, String)> {
    let name_node = node.child_by_field_name("name")?;
    let name = node_text(name_node, source);
    if name.is_empty() {
        return None;
    }
    let prefix = node
        .child_by_field_name("initializer")
        .and_then(|initializer| map_group_prefix_in_node(initializer, source))
        .or_else(|| map_group_prefix_in_node(node, source))?;
    Some((name, prefix))
}

fn map_group_prefix_in_node(node: Node<'_>, source: &str) -> Option<String> {
    if node.kind() == "invocation_expression"
        && let Some(prefix) = map_group_prefix_from_invocation(node, source)
    {
        return Some(prefix);
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(prefix) = map_group_prefix_in_node(child, source) {
            return Some(prefix);
        }
    }
    None
}

fn grpc_client_from_initializer(node: Node<'_>, source: &str) -> Option<(String, Option<String>)> {
    if node.kind() == "object_creation_expression"
        && let Some(result) = grpc_client_from_object_creation(node, source)
    {
        return Some(result);
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if let Some(result) = grpc_client_from_initializer(child, source) {
            return Some(result);
        }
    }
    None
}

/// `new {prefix.}{Service}Client(...)` -> `(service, prefix)` — same
/// `(service, prefix)` shape `grpc_service_from_base` returns on the impl
/// side, so `grpc_call_edge` can feed both through the same
/// `grpc_package_candidates_from_prefix`. Mirrors the impl side's
/// `X.XBase` self-reference requirement exactly (see
/// `split_client_service_and_prefix`) — this is also, deliberately, the
/// *only* way this ever returns `Some` for a bare, unqualified
/// `new WhateverClient(...)`, which is what an Azure SDK client
/// (`BlobServiceClient`, `SecretClient`, `ServiceBusClient`, ...) or the
/// Microsoft Graph SDK's `GraphServiceClient` always looks like in real
/// code (confirmed against dpb: every non-gRPC `...Client` construction in
/// that repo is either bare or, when fully qualified, doesn't carry the
/// `{Service}.{Service}Client` stutter) — see that function's doc for why
/// requiring it is the corroborating signal.
fn grpc_client_from_object_creation(
    node: Node<'_>,
    source: &str,
) -> Option<(String, Option<String>)> {
    if node.kind() != "object_creation_expression" {
        return None;
    }
    let type_node = node.child_by_field_name("type")?;
    let type_name = node_text(type_node, source);
    split_client_service_and_prefix(type_name.trim())
}

/// Strip a trailing `Client` suffix (generated gRPC client type convention)
/// from a dot-qualified type/receiver text, returning `(service, prefix)`
/// where `prefix` is whatever dotted segments preceded the
/// `{Service}.{Service}Client` pair, *excluding* that pair itself.
///
/// The `{Service}.` segment immediately before `{Service}Client` is
/// mandatory, not optional — grpc-csharp always nests the generated client
/// class inside a `{Service}` wrapper class of the exact same name
/// (`Greeter.GreeterClient`), so requiring that literal self-reference
/// stutter, exactly as `grpc_service_from_base` already requires it for
/// `{Service}.{Service}Base` on the impl side, is what tells a real
/// generated gRPC client apart from an unrelated `...Client`-suffixed SDK
/// type. This used to be optional here ("any qualifying prefix is taken at
/// face value"), which is exactly what let every Azure SDK / Graph SDK
/// client in dpb (`BlobServiceClient`, `SecretClient`, `ServiceBusClient`,
/// `GraphServiceClient`, ...) masquerade as a gRPC client and fan out one
/// bogus RPC_CALL candidate per bare `using` in its file — see
/// `grpc_call_edge`'s doc. A bare, unqualified type (no `.` at all — how
/// every one of those SDK types is actually constructed in dpb) can never
/// carry the stutter, so it's rejected up front the same way
/// `grpc_service_from_base` rejects a bare `XBase`.
///
/// `None` when the stutter isn't present. Shared by both client-detection
/// paths: a `new {prefix.}{Service}.{Service}Client(...)` construction, a
/// declared field/local type of that same shape (`collect_class_level_grpc_client_fields`
/// / `collect_grpc_clients_from_declaration`), and a call-site receiver
/// that is itself an inline construction (`grpc_service_from_client_receiver`).
fn split_client_service_and_prefix(text: &str) -> Option<(String, Option<String>)> {
    let text = text.trim();
    if text.is_empty() || !text.contains('.') {
        return None;
    }
    let mut parts: Vec<&str> = text.split('.').map(str::trim).collect();
    let last = parts.pop()?;
    let last = last.split('<').next().unwrap_or(last).trim();
    let service = if let Some(service) = last.strip_suffix("Client") {
        service
    } else {
        let lower = last.to_ascii_lowercase();
        if lower.ends_with("client") && last.len() > "client".len() {
            &last[..last.len() - "client".len()]
        } else {
            return None;
        }
    };
    if service.is_empty() {
        return None;
    }
    // Mandatory self-reference stutter — see the doc comment above.
    let prev = parts.pop()?;
    if prev != service {
        return None;
    }
    let prefix = parts
        .into_iter()
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>();
    let prefix = if prefix.is_empty() {
        None
    } else {
        Some(prefix.join("."))
    };
    Some((service.to_string(), prefix))
}

fn http_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    if node.kind() != "invocation_expression" {
        return None;
    }
    let target_node = node.child_by_field_name("function")?;
    let target = call_target_parts(target_node, source)?;
    let client = http_client_label(target.receiver.as_deref(), &target.full)?;
    let args = call_arguments(node);
    let (method, raw_path) = if target.name == "SendAsync" || target.name == "Send" {
        http_request_message_parts(args.first().copied()?, source)?
    } else if let Some(method) = normalize_http_method_name(&target.name) {
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

/// Builds an `RPC_IMPL` edge per candidate protobuf package in
/// `ctx.grpc_package_candidates` (see `grpc_package_candidates_from_prefix`),
/// not just one: a bare `using`-brought proto namespace can't be
/// disambiguated from other bare `using`s in the same file without a
/// whole-repo symbol table this single-file extractor doesn't have (see
/// `import_qualified_candidates` for the same trade-off on the CALLS side).
/// This is safe here in a way it isn't for CALLS resolution: an `RPC_IMPL`
/// edge only ever links up with a real `RPC_ROUTE` edge when its
/// `target_qualname` exactly matches one built from an actual `.proto`
/// package+service+rpc (see `proto::normalize_rpc_path`), so a wrong
/// candidate simply never matches anything downstream — it can't bind to
/// the wrong real route the way an over-eager CALLS edge could.
/// Deduplicates identical targets (e.g. two candidate packages that
/// normalize the same way).
fn grpc_impl_edge(node: Node<'_>, ctx: &Context, source: &str, rpc_name: &str) -> Vec<EdgeInput> {
    let Some(service) = ctx.grpc_service.as_deref() else {
        return Vec::new();
    };
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    let source_qualname = build_qualname(ctx, rpc_name);
    // ponytail: when no candidate package could be derived at all (no
    // base-list prefix and no bare `using` in the file), fall back to a
    // single package-less candidate rather than emitting nothing — covers
    // a proto file with no `package` statement, and top-level impl classes.
    // Upgrade path: none needed unless a real cross-file symbol table (like
    // `db::resolve_import_candidate`'s) becomes available to this
    // single-file extractor.
    let packages: Vec<Option<&str>> = if ctx.grpc_package_candidates.is_empty() {
        vec![None]
    } else {
        ctx.grpc_package_candidates
            .iter()
            .map(|p| Some(p.as_str()))
            .collect()
    };
    let mut seen_targets = std::collections::HashSet::new();
    let mut edges = Vec::new();
    for package in packages {
        let Some((raw_path, normalized)) = proto::normalize_rpc_path(package, service, rpc_name)
        else {
            continue;
        };
        if !seen_targets.insert(normalized.clone()) {
            continue;
        }
        let detail = json!({
            "framework": "grpc-csharp",
            "role": "server",
            "service": service,
            "rpc": rpc_name,
            "package": package,
            "raw": raw_path,
        })
        .to_string();
        edges.push(EdgeInput {
            kind: proto::RPC_IMPL_KIND.to_string(),
            source_qualname: Some(source_qualname.clone()),
            target_qualname: Some(normalized),
            detail: Some(detail),
            evidence_snippet: snippet.clone(),
            evidence_start_line: Some(start_line),
            evidence_end_line: Some(end_line),
            ..Default::default()
        });
    }
    edges
}

/// Builds an `RPC_CALL` edge per candidate `(service, protobuf package)`
/// pair, mirroring `grpc_impl_edge`'s treatment of `RPC_IMPL` (see that
/// function's doc) — the client side had the identical CLR-namespace bug
/// `1c83726` fixed for the impl side: the generated `{Service}.{Service}Client`
/// type's own qualifying prefix (from `new {prefix.}{Service}.{Service}Client(...)`,
/// captured by `grpc_client_from_object_creation` and carried in
/// `ctx.grpc_clients`), not `ctx.namespace_stack` (the *calling* code's own
/// CLR namespace, which has no reliable relationship to the proto package a
/// client it happens to construct belongs to), is what determines the
/// package. Reuses `grpc_package_candidates_from_prefix` rather than
/// duplicating its alias-resolution/bare-`using`-fallback logic.
///
/// Candidate *services* (plural) only when resolution fell through to
/// `grpc_service_from_client_binding`'s cross-file registry fallback — see
/// `GrpcClientFieldRegistry`'s doc for why that one's inherently ambiguous
/// (the receiver's own declaring type isn't visible from here) and why
/// fanning out over every one of them, rather than picking a winner, is
/// still safe: crossed with candidate packages below, a wrong `(service,
/// package)` pair just never matches a real `RPC_IMPL`/`RPC_ROUTE` target
/// downstream, exactly like a wrong candidate package alone already didn't.
fn grpc_call_edge(node: Node<'_>, ctx: &Context, source: &str) -> Vec<EdgeInput> {
    if node.kind() != "invocation_expression" {
        return Vec::new();
    }
    let Some(target_node) = node.child_by_field_name("function") else {
        return Vec::new();
    };
    let Some(target) = call_target_parts(target_node, source) else {
        return Vec::new();
    };
    let Some(rpc_name) = normalize_grpc_method_name(&target.name) else {
        return Vec::new();
    };
    let services: Vec<(String, Option<String>)> =
        match grpc_service_from_client_receiver(target.receiver.as_deref()) {
            Some(service_and_prefix) => vec![service_and_prefix],
            None => grpc_service_from_client_binding(target.receiver.as_deref(), ctx),
        };
    if services.is_empty() {
        return Vec::new();
    }
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    let source_qualname = ctx.current_scope.clone();
    let mut seen_targets = std::collections::HashSet::new();
    let mut edges = Vec::new();
    for (service, prefix) in services {
        // Same ponytail fallback as `grpc_impl_edge`: no derivable prefix
        // and no bare `using` in the file still emits one package-less
        // candidate rather than nothing, covering a proto file with no
        // `package` statement.
        let packages: Vec<Option<String>> =
            match grpc_package_candidates_from_prefix(prefix.as_deref(), ctx) {
                candidates if candidates.is_empty() => vec![None],
                candidates => candidates.into_iter().map(Some).collect(),
            };
        for package in packages {
            let Some((raw_path, normalized)) =
                proto::normalize_rpc_path(package.as_deref(), &service, &rpc_name)
            else {
                continue;
            };
            if !seen_targets.insert(normalized.clone()) {
                continue;
            }
            let detail = json!({
                "framework": "grpc-csharp",
                "role": "client",
                "service": service,
                "rpc": rpc_name,
                "package": package,
                "raw": raw_path,
            })
            .to_string();
            edges.push(EdgeInput {
                kind: proto::RPC_CALL_KIND.to_string(),
                source_qualname: Some(source_qualname.clone()),
                target_qualname: Some(normalized),
                detail: Some(detail),
                evidence_snippet: snippet.clone(),
                evidence_start_line: Some(start_line),
                evidence_end_line: Some(end_line),
                ..Default::default()
            });
        }
    }
    edges
}

fn channel_publish_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    if node.kind() != "invocation_expression" {
        return None;
    }
    let target_node = node.child_by_field_name("function")?;
    let target = call_target_parts(target_node, source)?;
    if !channel::is_publish_method(&target.name) {
        return None;
    }
    if !channel::is_bus_receiver(target.receiver.as_deref().unwrap_or("")) {
        return None;
    }
    let args = call_arguments(node);
    let first_arg = args.first()?;
    let raw_topic = node_text(*first_arg, source);
    let normalized = channel::normalize_channel_name(&raw_topic)?;
    let detail = channel::build_publish_detail(&normalized, &raw_topic, "azure-service-bus");
    Some(EdgeInput {
        kind: channel::CHANNEL_PUBLISH_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn channel_subscribe_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    if node.kind() != "invocation_expression" {
        return None;
    }
    let target_node = node.child_by_field_name("function")?;
    let target = call_target_parts(target_node, source)?;
    if !channel::is_subscribe_method(&target.name) {
        return None;
    }
    if !channel::is_bus_receiver(target.receiver.as_deref().unwrap_or("")) {
        return None;
    }
    let args = call_arguments(node);
    let first_arg = args.first()?;
    let raw_topic = node_text(*first_arg, source);
    let normalized = channel::normalize_channel_name(&raw_topic)?;
    let detail = channel::build_subscribe_detail(&normalized, &raw_topic, "azure-service-bus");
    Some(EdgeInput {
        kind: channel::CHANNEL_SUBSCRIBE_KIND.to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: Some(span(node).0),
        evidence_end_line: Some(span(node).2),
        ..Default::default()
    })
}

fn normalize_grpc_method_name(name: &str) -> Option<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(base) = trimmed.strip_suffix("Async")
        && !base.is_empty()
    {
        return Some(base.to_string());
    }
    Some(trimmed.to_string())
}

/// Finds the class's gRPC service base (e.g. `DeployerService.DeployerServiceBase`
/// or `DsDeploy.DeployerService.DeployerServiceBase`), returning
/// `(service_name, prefix)` where `prefix` is whatever base-list text comes
/// before the `<Service>.<Service>Base` pair — `None` for a bare base, or
/// the raw dotted/aliased text otherwise. See `grpc_service_from_base`.
fn grpc_service_from_bases(node: Node<'_>, source: &str) -> Option<(String, Option<String>)> {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "base_list" {
            continue;
        }
        let bases = base_list_types(child, source);
        for base in bases {
            if let Some(result) = grpc_service_from_base(&base) {
                return Some(result);
            }
        }
    }
    None
}

fn grpc_service_from_base(base: &str) -> Option<(String, Option<String>)> {
    let trimmed = base.trim();
    if trimmed.is_empty() || !trimmed.contains('.') {
        return None;
    }
    let mut parts: Vec<&str> = trimmed.split('.').map(str::trim).collect();
    let last = parts.pop()?;
    let last = last.split('<').next().unwrap_or(last).trim();
    if !last.ends_with("Base") {
        return None;
    }
    let service = last.trim_end_matches("Base");
    if service.is_empty() {
        return None;
    }
    let prev = parts.pop()?;
    if prev != service {
        return None;
    }
    let prefix = if parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
    };
    Some((service.to_string(), prefix))
}

/// Candidate protobuf package names for a gRPC service impl class, derived
/// from its base-list entry's namespace `prefix` (see
/// `grpc_service_from_base`) plus this file's `using` directives —
/// deliberately *not* from `ctx.namespace_stack` (the impl class's own CLR
/// namespace), which is the wrong signal this replaces: it's chosen for the
/// implementation's code organization, not the proto package it implements.
///
/// `Some(prefix)`: an alias (`using DsDeploy = Datasource.Deployer.V1;`)
/// resolves to its target and is the sole candidate (an alias can only ever
/// mean one thing). A non-alias prefix — already a dotted namespace written
/// directly in the base list — is used verbatim as the sole candidate.
///
/// `None` (bare `ServiceName.ServiceNameBase`, namespace brought in scope by
/// a bare `using ns;`): every bare `using` in the file is a candidate. This
/// never picks a winner among them — see `grpc_impl_edge`'s doc comment for
/// why an RPC_IMPL/RPC_ROUTE mismatch is harmless, unlike the CALLS-edge
/// ambiguity `import_qualified_candidates` guards against.
fn grpc_package_candidates_from_prefix(prefix: Option<&str>, ctx: &Context) -> Vec<String> {
    if let Some(prefix) = prefix {
        if let Some(fqn) = ctx.imports.aliases.get(prefix) {
            return vec![fqn.clone()];
        }
        return vec![prefix.to_string()];
    }
    let mut seen = std::collections::HashSet::new();
    let mut candidates = Vec::new();
    for ns in &ctx.imports.namespaces {
        if !ns.is_empty() && seen.insert(ns.clone()) {
            candidates.push(ns.clone());
        }
    }
    candidates
}

fn grpc_service_from_client_receiver(receiver: Option<&str>) -> Option<(String, Option<String>)> {
    let mut value = receiver?.trim().to_string();
    if value.is_empty() {
        return None;
    }
    if let Some(idx) = value.find('(') {
        value.truncate(idx);
    }
    value = value.trim_start_matches("new ").trim().to_string();
    split_client_service_and_prefix(&value)
}

/// Resolves a call-site receiver to every candidate `(service, prefix)` it
/// could name, trying — in order — an exact match against this file's own
/// `ctx.grpc_clients` (locally-bound variable, or a field/property of the
/// directly enclosing type), then the receiver's trailing segment against
/// the same map (`obj.client.Method()`-style single-hop field access within
/// this file), then that trailing segment against the cross-file
/// `ctx.grpc_client_fields` registry — see that type's doc for why this
/// last tier can return more than one candidate and why that's still safe.
/// The first tier to produce anything wins; later tiers are strictly less
/// precise so they're only consulted once an earlier one comes up empty.
fn grpc_service_from_client_binding(
    receiver: Option<&str>,
    ctx: &Context,
) -> Vec<(String, Option<String>)> {
    let Some(receiver) = receiver.map(str::trim).filter(|r| !r.is_empty()) else {
        return Vec::new();
    };
    if let Some(service_and_prefix) = ctx.grpc_clients.get(receiver) {
        return vec![service_and_prefix.clone()];
    }
    let last = receiver.rsplit('.').next().unwrap_or(receiver);
    if let Some(service_and_prefix) = ctx.grpc_clients.get(last) {
        return vec![service_and_prefix.clone()];
    }
    ctx.grpc_client_fields
        .borrow()
        .get(last)
        .cloned()
        .unwrap_or_default()
}

fn http_request_message_parts(node: Node<'_>, source: &str) -> Option<(String, String)> {
    if node.kind() != "object_creation_expression" {
        return None;
    }
    let type_node = node.child_by_field_name("type")?;
    let type_name = node_text(type_node, source);
    if !type_name.ends_with("HttpRequestMessage") {
        return None;
    }
    let args = node
        .child_by_field_name("arguments")
        .map(argument_values)
        .unwrap_or_default();
    let method = args
        .first()
        .and_then(|arg| extract_method_from_expr(*arg, source))?;
    let raw_path = args
        .get(1)
        .and_then(|arg| extract_string_literal(*arg, source))?;
    Some((method, raw_path))
}

fn call_arguments(node: Node<'_>) -> Vec<Node<'_>> {
    node.child_by_field_name("arguments")
        .map(argument_values)
        .unwrap_or_default()
}

fn argument_values(node: Node<'_>) -> Vec<Node<'_>> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "argument" {
            continue;
        }
        if let Some(expr) = argument_expr(child) {
            out.push(expr);
        }
    }
    out
}

fn argument_expr(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    let mut expr = None;
    for child in node.named_children(&mut cursor) {
        expr = Some(child);
    }
    expr
}

fn call_target_parts(node: Node<'_>, source: &str) -> Option<CallTarget> {
    let full = node_text(node, source);
    if full.is_empty() {
        return None;
    }
    if node.kind() == "member_access_expression" {
        let receiver = node
            .child_by_field_name("expression")
            .map(|expr| node_text(expr, source))
            .filter(|value| !value.is_empty());
        let name = node
            .child_by_field_name("name")
            .map(|name| node_text(name, source))
            .unwrap_or_else(|| full.clone());
        return Some(CallTarget {
            receiver,
            name,
            full,
        });
    }
    let (receiver, name) = split_last_segment(&full);
    Some(CallTarget {
        receiver,
        name,
        full,
    })
}

fn split_last_segment(raw: &str) -> (Option<String>, String) {
    if let Some((left, right)) = raw.rsplit_once('.') {
        return (Some(left.to_string()), right.to_string());
    }
    (None, raw.to_string())
}

fn handler_name_from_expr(node: Node<'_>, ctx: &Context, source: &str) -> Option<String> {
    let raw = node_text(node, source);
    if raw.is_empty() {
        return None;
    }
    resolve_call_target(&raw, ctx).or(Some(raw))
}

fn normalize_http_method_name(name: &str) -> Option<String> {
    if let Some(method) = http::normalize_method(name) {
        return Some(method);
    }
    let mut trimmed = name.to_string();
    for suffix in ["FromJsonAsync", "AsJsonAsync", "JsonAsync", "Async"] {
        if trimmed.ends_with(suffix) {
            let end = trimmed.len() - suffix.len();
            trimmed.truncate(end);
            break;
        }
    }
    http::normalize_method(&trimmed)
}

fn extract_method_from_expr(node: Node<'_>, source: &str) -> Option<String> {
    if let Some(raw) = extract_string_literal(node, source) {
        return http::normalize_method(&raw);
    }
    let raw = node_text(node, source);
    if raw.is_empty() {
        return None;
    }
    let last = raw.rsplit('.').next().unwrap_or(raw.as_str());
    http::normalize_method(last)
}

fn extract_string_literal(node: Node<'_>, source: &str) -> Option<String> {
    match node.kind() {
        "string_literal" | "verbatim_string_literal" | "raw_string_literal" => {
            let raw = node_text(node, source);
            unquote_string_literal(&raw)
        }
        _ => None,
    }
}

fn extract_string_list(node: Node<'_>, source: &str) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(value) = extract_string_literal(node, source) {
        out.push(value);
        return out;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        out.extend(extract_string_list(child, source));
    }
    out
}

fn unquote_string_literal(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(rest) = trimmed.strip_prefix("@\"")
        && let Some(value) = rest.strip_suffix('"')
    {
        return Some(value.replace("\"\"", "\""));
    }
    let quote_count = trimmed.chars().take_while(|ch| *ch == '"').count();
    if quote_count >= 3 && trimmed.ends_with(&"\"".repeat(quote_count)) {
        let start = quote_count;
        let end = trimmed.len() - quote_count;
        if start <= end {
            return Some(trimmed[start..end].to_string());
        }
    }
    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
        return Some(trimmed[1..trimmed.len() - 1].to_string());
    }
    None
}

/// Extract content between outermost `<>` with nesting support.
/// `"Configure<DatabaseOptions>"` → `Some("DatabaseOptions")`
fn extract_generic_type_arg(text: &str) -> Option<String> {
    let start = text.find('<')?;
    let mut depth = 0;
    let mut end = None;
    for (i, ch) in text.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(i);
                    break;
                }
            }
            _ => {}
        }
    }
    let end = end?;
    let inner = text[start + 1..end].trim();
    if inner.is_empty() {
        return None;
    }
    Some(inner.to_string())
}

/// Check if type_text is IOptions<T>, IOptionsMonitor<T>, or IOptionsSnapshot<T>.
/// Returns the inner type T.
fn extract_options_type(type_text: &str) -> Option<String> {
    let trimmed = type_text.trim();
    for prefix in &["IOptions<", "IOptionsMonitor<", "IOptionsSnapshot<"] {
        if trimmed.starts_with(prefix) {
            return extract_generic_type_arg(trimmed);
        }
    }
    None
}

/// Split a string on commas, respecting nested `<>` brackets.
fn split_respecting_brackets(text: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, ch) in text.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(text[start..i].to_string());
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(text[start..].to_string());
    parts
}

/// Extract the type part from a parameter declaration like `IOptions<DatabaseOptions> db`.
/// Returns just the type (everything before the last whitespace-separated token).
fn param_type_part(param: &str) -> Option<String> {
    let trimmed = param.trim();
    if trimmed.is_empty() {
        return None;
    }
    // Find last space that's not inside <>
    let mut depth = 0;
    let mut last_space = None;
    for (i, ch) in trimmed.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => depth -= 1,
            ' ' | '\t' if depth == 0 => last_space = Some(i),
            _ => {}
        }
    }
    let type_part = match last_space {
        Some(idx) => &trimmed[..idx],
        None => trimmed,
    };
    let type_part = type_part.trim();
    if type_part.is_empty() {
        return None;
    }
    Some(type_part.to_string())
}

/// Parse raw constructor parameter text and extract IOptions<T> matches.
/// Returns `(options_type, wrapper_type)` pairs.
fn extract_di_options_types_from_params(param_text: &str) -> Vec<(String, String)> {
    let text = param_text
        .trim()
        .trim_start_matches('(')
        .trim_end_matches(')');
    let mut results = Vec::new();
    for part in split_respecting_brackets(text) {
        if let Some(type_text) = param_type_part(&part)
            && let Some(options_type) = extract_options_type(&type_text)
        {
            let wrapper = type_text
                .split('<')
                .next()
                .unwrap_or(&type_text)
                .to_string();
            results.push((options_type, wrapper));
        }
    }
    results
}

fn http_client_label(receiver: Option<&str>, full: &str) -> Option<&'static str> {
    let full_lower = full.to_ascii_lowercase();
    let receiver_lower = receiver.unwrap_or("").to_ascii_lowercase();
    if full_lower.contains("httpclient") || receiver_lower.contains("httpclient") {
        return Some("httpclient");
    }
    if receiver_lower.ends_with("client") || receiver_lower.contains("client") {
        return Some("http_client");
    }
    None
}

fn call_target_node(node: Node<'_>) -> Option<Node<'_>> {
    node.child_by_field_name("expression")
        .or_else(|| node.child_by_field_name("function"))
        .or_else(|| node.child_by_field_name("constructor"))
        .or_else(|| node.child_by_field_name("type"))
}

fn resolve_call_target(raw: &str, ctx: &Context) -> Option<String> {
    let raw = collapse_call_target_whitespace(raw);
    let raw = raw.as_str();
    if raw.is_empty() || !is_simple_call_target(raw) {
        return None;
    }
    if let Some(rest) = raw.strip_prefix("this.") {
        let container = container_qualname(ctx);
        if container.is_empty() {
            return Some(rest.to_string());
        }
        return Some(format!("{container}.{rest}"));
    }
    if let Some(rest) = raw.strip_prefix("base.") {
        let container = container_qualname(ctx);
        if container.is_empty() {
            return Some(rest.to_string());
        }
        return Some(format!("{container}.{rest}"));
    }
    if !raw.contains('.') {
        let container = container_qualname(ctx);
        if container.is_empty() {
            return Some(raw.to_string());
        }
        return Some(format!("{container}.{raw}"));
    }
    Some(raw.to_string())
}

fn is_simple_call_target(raw: &str) -> bool {
    raw.chars()
        .all(|ch| ch.is_alphanumeric() || ch == '_' || ch == '.' || ch == '$' || ch == '@')
}

fn is_nested_function_node(kind: &str) -> bool {
    matches!(
        kind,
        "local_function_statement"
            | "anonymous_method_expression"
            | "lambda_expression"
            | "parenthesized_lambda_expression"
            | "simple_lambda_expression"
    )
}

fn walk_declaration_list(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

/// C# convention: interface names start with `I` followed by an uppercase letter.
/// Uses the last segment of a potentially qualified name (e.g., `Foo.IBar` → `IBar`).
fn is_likely_interface_name(name: &str) -> bool {
    let last = name.rsplit('.').next().unwrap_or(name);
    let last = last.split('<').next().unwrap_or(last);
    let mut chars = last.chars();
    matches!(chars.next(), Some('I')) && matches!(chars.next(), Some(c) if c.is_uppercase())
}

fn handle_base_list(
    node: Node<'_>,
    qualname: &str,
    source: &str,
    output: &mut ExtractedFile,
    kind: TypeKind,
) {
    let mut cursor = node.walk();
    let mut bases = Vec::new();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "base_list" {
            continue;
        }
        bases.extend(base_list_types(child, source));
    }
    if bases.is_empty() {
        return;
    }
    match kind {
        TypeKind::Class | TypeKind::Record => {
            let mut iter = bases.into_iter();
            if let Some(base) = iter.next() {
                // C# convention: interfaces start with I + uppercase letter.
                // If the first base looks like an interface, emit IMPLEMENTS.
                let edge_kind = if is_likely_interface_name(&base) {
                    "IMPLEMENTS"
                } else {
                    "EXTENDS"
                };
                output.edges.push(EdgeInput {
                    kind: edge_kind.to_string(),
                    source_qualname: Some(qualname.to_string()),
                    target_qualname: Some(base),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
            for iface in iter {
                output.edges.push(EdgeInput {
                    kind: "IMPLEMENTS".to_string(),
                    source_qualname: Some(qualname.to_string()),
                    target_qualname: Some(iface),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
        }
        TypeKind::Interface => {
            for iface in bases {
                output.edges.push(EdgeInput {
                    kind: "EXTENDS".to_string(),
                    source_qualname: Some(qualname.to_string()),
                    target_qualname: Some(iface),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
        }
        TypeKind::Struct | TypeKind::Enum => {
            for iface in bases {
                output.edges.push(EdgeInput {
                    kind: "IMPLEMENTS".to_string(),
                    source_qualname: Some(qualname.to_string()),
                    target_qualname: Some(iface),
                    detail: None,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
        }
    }
}

fn base_list_types(node: Node<'_>, source: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        match child.kind() {
            "argument_list" => {}
            "primary_constructor_base_type" => {
                if let Some(type_node) = child.child_by_field_name("type") {
                    let name = node_text(type_node, source);
                    if !name.is_empty() {
                        out.push(name);
                    }
                } else {
                    let name = node_text(child, source);
                    if !name.is_empty() {
                        out.push(name);
                    }
                }
            }
            _ => {
                let name = node_text(child, source);
                if !name.is_empty() {
                    out.push(name);
                }
            }
        }
    }
    out
}

fn namespace_name(node: Node<'_>, source: &str) -> Option<String> {
    node.child_by_field_name("name")
        .map(|n| node_text(n, source))
        .filter(|value| !value.is_empty())
}

fn namespace_parts(name: &str) -> Vec<String> {
    let normalized = name.replace("::", ".");
    normalized
        .split('.')
        .filter(|part| !part.trim().is_empty())
        .map(|part| part.trim().to_string())
        .collect()
}

fn type_signature(node: Node<'_>, source: &str) -> Option<String> {
    node.child_by_field_name("parameters")
        .map(|n| node_text(n, source))
        .filter(|value| !value.is_empty())
        .or_else(|| {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if child.kind() == "parameter_list" {
                    let value = node_text(child, source);
                    if !value.is_empty() {
                        return Some(value);
                    }
                }
            }
            None
        })
}

fn method_signature(node: Node<'_>, source: &str) -> Option<String> {
    let params = node
        .child_by_field_name("parameters")
        .map(|n| node_text(n, source));
    let params = match params {
        Some(value) if !value.is_empty() => value,
        _ => return None,
    };
    let returns = node
        .child_by_field_name("returns")
        .map(|n| node_text(n, source))
        .filter(|value| !value.is_empty());
    match returns {
        Some(ret) => Some(format!("{params} -> {ret}")),
        None => Some(params),
    }
}

fn base_qualname(ctx: &Context) -> String {
    if !ctx.namespace_stack.is_empty() {
        ctx.namespace_stack.join(".")
    } else {
        ctx.module.clone()
    }
}

fn build_qualname(ctx: &Context, name: &str) -> String {
    let mut parts = Vec::new();
    let base = base_qualname(ctx);
    if !base.is_empty() {
        parts.push(base);
    }
    if !ctx.type_stack.is_empty() {
        parts.push(ctx.type_stack.join("."));
    }
    parts.push(name.to_string());
    parts.join(".")
}

fn container_qualname(ctx: &Context) -> String {
    let base = base_qualname(ctx);
    if ctx.type_stack.is_empty() {
        base
    } else if base.is_empty() {
        ctx.type_stack.join(".")
    } else {
        format!("{base}.{}", ctx.type_stack.join("."))
    }
}

const CS_BUILTIN_TYPES: &[&str] = &[
    "bool",
    "byte",
    "sbyte",
    "char",
    "decimal",
    "double",
    "float",
    "int",
    "uint",
    "long",
    "ulong",
    "short",
    "ushort",
    "object",
    "string",
    "void",
    "dynamic",
    "var",
    "Task",
    "ValueTask",
    "Array",
    "String",
    "Object",
    "Exception",
    "List",
    "Dictionary",
    "IEnumerable",
    "IList",
    "ICollection",
    "IReadOnlyList",
    "IReadOnlyCollection",
    "IReadOnlyDictionary",
    "HashSet",
    "Queue",
    "Stack",
    "DateTime",
    "DateTimeOffset",
    "TimeSpan",
    "Guid",
    "Uri",
    "StringBuilder",
    "Nullable",
    "Tuple",
    "ValueTuple",
    "Action",
    "Func",
    "EventHandler",
    "CancellationToken",
    "Type",
    "Random",
];

/// Infer the receiver type of a call's callee expression (`function_node`),
/// mirroring `python::infer_receiver_type` with `this`/`base` standing in
/// for `self`/`cls`. Only gates resolution; never changes `target_qualname`
/// (see `resolve_call_target`, which stays text-based and keeps the
/// receiver's literal text for evidence).
///
/// Rules, in order:
/// - Not a member access at all (`Helper()`) → `NotTracked` (bare call,
///   nothing to gate).
/// - `base.Method()` (zero hops) → `Known`/`Unresolved` from the enclosing
///   type's own resolvable base class (`Context::base_type`); unlike
///   `this`, `resolve_call_target`'s exact-looking `{currentClass}.Method`
///   guess is frequently wrong for `base.` calls (the whole point of
///   calling `base.` is usually that the current class does *not* define
///   its own override), so this is gated even at zero hops.
/// - `base.field.Method()` (any deeper hop) → `Unresolved`: the base
///   type's own field types aren't available to a single-file extractor.
/// - `this.Method()` (zero hops) → `NotTracked`, already resolved exactly
///   via `resolve_call_target`'s container-qualname path.
/// - `this.Field.Method()` / `this.Property.Method()` (exactly one hop off
///   `this`) → resolved via a type-annotated field or property, if any;
///   otherwise `Unresolved`.
/// - `X.Method()` where `X` is a bare identifier: `Known`/`Unresolved` from
///   this method's local types if `X` is tracked, else `NotTracked` (a
///   static/class reference, e.g. `Console.WriteLine()`).
/// - Anything deeper, or a chain rooted in something other than a bare
///   identifier/`this`/`base` (a call result, a cast, ...), → `Unresolved`
///   if the root is `this` or a tracked local, `NotTracked` otherwise.
fn infer_receiver_type(function_node: Node<'_>, source: &str, ctx: &Context) -> ReceiverType {
    if function_node.kind() != "member_access_expression" {
        return ReceiverType::NotTracked;
    }
    let Some(object) = function_node.child_by_field_name("expression") else {
        return ReceiverType::NotTracked;
    };
    let (root, hops) = member_access_root(object);

    if root.kind() == "base" {
        if hops == 0 {
            return match &ctx.base_type {
                LocalType::Known(ty) => ReceiverType::Known(ty.clone()),
                LocalType::Other => ReceiverType::Unresolved,
            };
        }
        // ponytail: `base.Field.Method()` would need the base type's own
        // field types, which may live in another file entirely — out of
        // reach for single-file, single-pass extraction.
        return ReceiverType::Unresolved;
    }

    if root.kind() == "this" {
        if hops == 0 {
            return ReceiverType::NotTracked;
        }
        if hops == 1 {
            let attr_name = object
                .child_by_field_name("name")
                .map(|n| node_text(n, source));
            return match attr_name.and_then(|name| ctx.class_attr_types.get(&name).cloned()) {
                Some(LocalType::Known(ty)) => ReceiverType::Known(ty),
                _ => ReceiverType::Unresolved,
            };
        }
        // ponytail: deeper chains (`this.a.b.Method()`) would need real
        // attribute-type inference across assignments — out of scope, same
        // ceiling as `python::infer_receiver_type`.
        return ReceiverType::Unresolved;
    }

    if root.kind() != "identifier" {
        // Chain rooted in a call result, cast expression, subscript, etc.
        // — not inferable.
        return ReceiverType::Unresolved;
    }
    let root_name = node_text(root, source);
    // C#, unlike TypeScript/Python, lets a method body reference an
    // instance *or static* field of its own class by its bare name, with
    // no `this.` prefix at all — and a `static` field can *only* ever be
    // reached that way (`this.` on a static member doesn't compile). So a
    // bare identifier is checked against `local_types` first (a local
    // shadows a same-named field, standard C# scoping), falling back to
    // `class_attr_types` — reusing the exact same field/property map
    // `this.field.Method()` already consults, just from an additional call
    // site.
    if let Some(local) = ctx.local_types.get(&root_name) {
        if hops == 0 {
            return match local {
                LocalType::Known(ty) => ReceiverType::Known(ty.clone()),
                LocalType::Other => ReceiverType::Unresolved,
            };
        }
        return ReceiverType::Unresolved;
    }
    if let Some(attr) = ctx.class_attr_types.get(&root_name) {
        if hops == 0 {
            return match attr {
                LocalType::Known(ty) => ReceiverType::Known(ty.clone()),
                LocalType::Other => ReceiverType::Unresolved,
            };
        }
        return ReceiverType::Unresolved;
    }
    ReceiverType::NotTracked
}

/// Walk a (possibly nested) member-access chain down to its root node,
/// returning the root plus how many hops separate it from `node` (0 =
/// `node` itself is the root).
fn member_access_root(node: Node<'_>) -> (Node<'_>, usize) {
    let mut current = node;
    let mut hops = 0;
    while current.kind() == "member_access_expression" {
        match current.child_by_field_name("expression") {
            Some(obj) => {
                current = obj;
                hops += 1;
            }
            None => break,
        }
    }
    (current, hops)
}

/// A file's `using` directives, collected once (see `collect_import_context`)
/// and consulted only to qualify a bare `Type.Method()` call's receiver into
/// candidate fully-qualified qualnames — see `import_qualified_candidates`.
/// Deliberately coarse: this is not a real name-resolution pass (it has no
/// notion of which types actually live in an imported namespace, since
/// that requires the whole-repo symbol table this single-file extractor
/// doesn't have access to). It only narrows *what to try*; the DB layer
/// (`Db::insert_edges` / `db::resolve_import_candidate`) is what actually
/// decides, against real symbols, whether a candidate is unambiguous.
#[derive(Debug, Default, Clone)]
struct ImportContext {
    /// Namespaces brought into scope via a bare `using NS;` directive, in
    /// order of appearance (duplicates harmless — deduped when building
    /// candidates). A receiver `X` is tried as `{ns}.X` for each of these.
    namespaces: Vec<String>,
    /// Alias -> fully-qualified target, from `using Alias = NS.Type;`. A
    /// receiver exactly matching a key here is qualified directly and is
    /// the *sole* candidate (an alias can only ever mean one thing, so it
    /// short-circuits the namespace-guessing path entirely).
    aliases: HashMap<String, String>,
}

/// Walk the whole file once, before the main symbol/edge walk, collecting
/// every `using_directive` node into an `ImportContext`. Import directives
/// don't nest meaningfully in real C# (block-scoped `using`s inside a
/// namespace are rare and, even then, apply to the whole file in every
/// codebase this extractor has been measured against) so this is a flat
/// scan rather than something threaded through `walk_node`'s per-scope
/// `Context` — see `Context::imports`, set once in `extract()` and never
/// mutated afterward.
fn collect_import_context(root: Node<'_>, source: &str) -> ImportContext {
    let mut ctx = ImportContext::default();
    collect_import_context_rec(root, source, &mut ctx);
    ctx
}

fn collect_import_context_rec(node: Node<'_>, source: &str, out: &mut ImportContext) {
    if node.kind() == "using_directive" {
        record_using_directive(node, source, out);
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_import_context_rec(child, source, out);
    }
}

fn record_using_directive(node: Node<'_>, source: &str, out: &mut ImportContext) {
    // `using static Type;` brings a *type's* members into scope directly
    // (so a bare `Method()` — not `Type.Method()` — could resolve through
    // it), which is a different shape than everything else this module
    // handles and isn't covered by the issue this exists to fix.
    // ponytail: not handled — see module doc. Upgrade path: track the
    // named type as an implicit extra receiver-free candidate, separate
    // from `namespaces`/`aliases` (both of which qualify a *receiver*).
    let text = node_text(node, source);
    let after_using = text
        .trim()
        .strip_prefix("global")
        .map(str::trim)
        .unwrap_or_else(|| text.trim())
        .strip_prefix("using")
        .map(str::trim)
        .unwrap_or("");
    if after_using.starts_with("static") {
        return;
    }

    // Alias form: `using Alias = Some.Qualified.Type;` — grammar gives the
    // alias its own `name` field; the RHS (whatever concrete shape —
    // `qualified_name`, `identifier`, `generic_name`, ...) is simply the
    // other named child, not wrapped in any distinguishing node kind (the
    // grammar's `type` rule is a supertype that never itself materializes
    // in the tree — confirmed via `tree.root_node().to_sexp()` on a real
    // alias directive, so this doesn't rely on `handle_using`'s `"type"`
    // check, which — for this same reason — never actually matches here
    // either; `handle_using` only works for this shape via its own
    // fallback loop).
    if let Some(alias_node) = node.child_by_field_name("name") {
        let alias = node_text(alias_node, source);
        if alias.is_empty() {
            return;
        }
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            if child.id() == alias_node.id() {
                continue;
            }
            let target = node_text(child, source);
            if !target.is_empty() {
                out.aliases.insert(alias, target);
            }
            return;
        }
        return;
    }

    // Plain form: `using Some.Namespace;` — the target is a direct named
    // child (qualified_name/identifier/generic_name/alias_qualified_name),
    // same shape `handle_using`'s fallback loop already matches.
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if matches!(
            child.kind(),
            "qualified_name" | "identifier" | "generic_name" | "alias_qualified_name"
        ) {
            let name = node_text(child, source);
            if !name.is_empty() {
                out.namespaces.push(name);
            }
            return;
        }
    }
}

/// Split a call's raw target text into `(receiver, suffix)` when its first
/// (`.`-delimited) segment looks like a C# type name (starts with an
/// uppercase letter) and is itself a plain identifier — not `this`/`base`,
/// not a generic/indexer/call expression. `suffix` is everything after
/// that first segment's dot and may itself be dotted (`CheckDuration.Record`
/// for `HealthMeters.CheckDuration.Record`) — a static member access can be
/// chained arbitrarily deep (`Type.Field.Method()`, `Type.Nested.Method()`),
/// and every one of those shapes is exactly as much "this call is rooted at
/// a type name" evidence as the plain two-segment case. Anything else
/// returns `None` (no import qualification attempted) — see
/// `import_qualified_candidates`'s caller.
///
/// The uppercase check matters: without it, an ordinary instance call like
/// `row.ToDomain()` also matches this shape (`row` is just as dot-free as
/// `UniqueName`), and `import_qualified_candidates` would then build
/// candidates like `{ns}.row.ToDomain` — guaranteed to miss (`row` isn't a
/// class), which previously did active harm: a non-empty but unresolvable
/// `import_candidates` list makes `Db::insert_edges` persist
/// `receiver_type = ""` (see `1d6a5a7`'s guard), permanently blocking the
/// bare-name fallback tier that would otherwise have had a real shot at
/// resolving the call correctly (or, for extension-method receivers,
/// blocking `extension_method_candidates` from being the sole source of
/// truth). C# identifier convention (locals/fields lowerCamelCase or
/// `_prefixed`, types PascalCase) makes this a cheap, reliable filter — this
/// tier exists specifically for static-access shapes rooted at a type name
/// (`UniqueName.Create()`, `HealthMeters.CheckDuration.Record()`), and every
/// real type name in C# starts uppercase.
///
/// Allowing a dotted suffix is itself a fix, not just a generalization: a
/// call like `HealthMeters.CheckDuration.Record(...)` (a static field's
/// value, `Record` called on the `Histogram<double>` it holds — see
/// `1d6a5a7`'s "positive evidence of an external receiver" reasoning) used
/// to produce *no* candidate at all here (the old two-segment-only version
/// rejected any dotted suffix outright), so the `1d6a5a7` guard never saw
/// evidence to act on and the call fell through unguarded to the bare-name
/// tier — which then wrongly bound it to any unrelated same-named `Record`
/// method elsewhere in the repo. The candidate this produces
/// (`{ns}.HealthMeters.CheckDuration.Record`) is essentially guaranteed to
/// find no real symbol either (nothing is nested under a field), which is
/// exactly the point: a non-empty, unresolvable candidate list is what
/// lets the existing guard correctly refuse to bind, instead of an empty
/// list that left the call looking like it had no receiver-type signal at
/// all.
fn type_prefixed_receiver_and_suffix(raw: &str) -> Option<(&str, &str)> {
    let (receiver, suffix) = raw.split_once('.')?;
    if receiver.is_empty() || suffix.is_empty() {
        return None;
    }
    if receiver == "this" || receiver == "base" {
        return None;
    }
    let mut chars = receiver.chars();
    let first = chars.next()?;
    if !first.is_uppercase() {
        return None;
    }
    if !receiver.chars().all(|ch| ch.is_alphanumeric() || ch == '_') {
        return None;
    }
    Some((receiver, suffix))
}

/// Compute fully-qualified candidate qualnames for a bare `Type.Method()`
/// (or deeper, `Type.Field.Method()`-shaped) call whose receiver `type_name`
/// is not a tracked local/field (i.e. `infer_receiver_type` returned
/// `NotTracked` for this call), using the file's import context plus its
/// current enclosing namespace. `suffix` is appended verbatim, dots and
/// all, so a two-segment call passes a bare method name and a deeper chain
/// passes its own dotted remainder unchanged — see
/// `type_prefixed_receiver_and_suffix`'s doc for why a candidate that's
/// bound to fail (a deeper chain very rarely names a real symbol) is still
/// exactly the useful output here.
///
/// An alias match is authoritative and the sole candidate returned (an
/// alias can only ever mean one thing). Otherwise, one candidate per
/// distinct namespace source that could plausibly supply `type_name`: the
/// call site's own enclosing namespace (a sibling type in the same
/// namespace needs no `using` at all), plus `{ns}.{type_name}.{suffix}`
/// for every bare `using ns;` directive in the file.
///
/// This never picks a winner among multiple namespace candidates — that's
/// the DB layer's job (`db::resolve_import_candidate`), which tries every
/// candidate against the real symbol table and binds only if exactly one
/// resolves; 0 or 2+ hits fall through unchanged to the pre-existing
/// two-segment/bare-name tiers. So an ambiguous `using` situation here
/// still ends up refused downstream, never guessed.
fn import_qualified_candidates(type_name: &str, suffix: &str, ctx: &Context) -> Vec<String> {
    if let Some(fqn) = ctx.imports.aliases.get(type_name) {
        return vec![format!("{fqn}.{suffix}")];
    }
    let mut seen = std::collections::HashSet::new();
    let mut candidates = Vec::new();
    let mut push = |ns: &str| {
        if ns.is_empty() {
            return;
        }
        let candidate = format!("{ns}.{type_name}.{suffix}");
        if seen.insert(candidate.clone()) {
            candidates.push(candidate);
        }
    };
    if !ctx.namespace_stack.is_empty() {
        push(&ctx.namespace_stack.join("."));
    }
    for ns in &ctx.imports.namespaces {
        push(ns);
    }
    candidates
}

/// If `node` (a `method_declaration` already known to have a non-empty
/// name/qualname) is a C# extension method — `static`, with a first
/// parameter carrying the `this` modifier — record it into
/// `ctx.extension_registry` under its bare method name. Every other method
/// is a no-op. See `Context::extension_registry` for why this exists and
/// `extension_method_candidates` for how it's consumed.
fn record_extension_method(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    name: &str,
    qualname: &str,
) {
    if !has_modifier(node, source, "static") {
        return;
    }
    let Some(params) = node.child_by_field_name("parameters") else {
        return;
    };
    let mut cursor = params.walk();
    let Some(first_param) = params
        .named_children(&mut cursor)
        .find(|c| c.kind() == "parameter")
    else {
        return;
    };
    if !has_modifier(first_param, source, "this") {
        return;
    }
    let receiver_type = first_param
        .child_by_field_name("type")
        .map(|t| classify_annotation(&node_text(t, source)))
        .and_then(|ty| match ty {
            LocalType::Known(name) => Some(name),
            LocalType::Other => None,
        });
    let namespace = ctx.namespace_stack.join(".");
    ctx.extension_registry
        .borrow_mut()
        .entry(name.to_string())
        .or_default()
        .push(ExtensionMethodEntry {
            qualname: qualname.to_string(),
            namespace,
            receiver_type,
        });
}

/// Whether `node` has a direct `modifier` child whose text is exactly
/// `keyword` — e.g. `has_modifier(method_node, source, "static")` or
/// `has_modifier(parameter_node, source, "this")`. Every C# modifier
/// (`public`, `static`, `this`, `readonly`, ...) parses to the same
/// `modifier` node kind wrapping a single keyword token, regardless of
/// which declaration it appears on — confirmed via a parse-tree dump of a
/// real extension method (`public static T Foo(this U u)`), same technique
/// `record_using_directive`'s doc references.
fn has_modifier(node: Node<'_>, source: &str, keyword: &str) -> bool {
    let mut cursor = node.walk();
    node.named_children(&mut cursor)
        .any(|c| c.kind() == "modifier" && node_text(c, source).trim() == keyword)
}

/// Whether `namespace` (an extension method's declaring namespace — see
/// `ExtensionMethodEntry::namespace`) is in scope for the *current* call
/// site: either the call site's own enclosing namespace (no `using`
/// needed — real C# lets sibling types in the same namespace see each
/// other), a namespace named by one of this file's bare `using ns;`
/// directives, or the target of a `using Alias = ns;` directive. Mirrors
/// the same namespace sources `import_qualified_candidates` already
/// consults, just checking membership instead of building guesses.
fn namespace_in_scope(namespace: &str, ctx: &Context) -> bool {
    if namespace.is_empty() {
        return false;
    }
    if !ctx.namespace_stack.is_empty() && ctx.namespace_stack.join(".") == namespace {
        return true;
    }
    if ctx.imports.namespaces.iter().any(|ns| ns == namespace) {
        return true;
    }
    ctx.imports.aliases.values().any(|fqn| fqn == namespace)
}

/// Candidate fully-qualified qualnames for a call that may be invoking an
/// *extension* method — `receiver.Method(...)`, where `Method` isn't
/// declared on the receiver's own type (or the receiver's type is unknown
/// entirely) but on some `static` class whose namespace this file has
/// imported. This is the mechanism the "extension methods are invisible to
/// CALLS resolution" defect needs fixed: unlike `import_qualified_candidates`
/// (which qualifies a *type-looking*
/// receiver into `{ns}.{receiver}.{method}`), an extension call's receiver
/// is an *instance* — its text is never the declaring class's name, so no
/// amount of namespace-guessing from the call site alone can construct the
/// declaring class's qualname. The only place that name is ever available
/// is the declaration itself, so this looks it up in
/// `ctx.extension_registry` (built by `record_extension_method` as this
/// extractor processes every file this run — see that field's doc).
///
/// Returns candidates only when there's positive, already-observed
/// evidence: every registry entry under `method_name` is filtered to those
/// (a) whose declaring namespace is in scope here (`namespace_in_scope`)
/// and (b) not positively *incompatible* with `receiver_type` (an entry
/// with a known, different receiver type is excluded; an entry with an
/// unclassifiable/generic receiver type, or a call whose own receiver type
/// isn't confidently known, is never excluded on this basis — see
/// `ExtensionMethodEntry::receiver_type`'s doc). An empty result here means
/// "no evidence either way", not "not an extension method" — the caller
/// must leave `import_candidates` empty in that case rather than pass
/// along a list that would (via the `1d6a5a7` guard) wrongly foreclose
/// every other resolution tier for what might just be an ordinary call.
///
/// ponytail: order-dependent within a single reindex — a call site in a
/// file processed *before* its extension method's declaring file gets no
/// candidate here (the registry entry doesn't exist yet). A full cold
/// reindex still ends up with the complete registry, so only the specific
/// pairing of (this call site's file, that method's declaring file)
/// processed in the "wrong" relative order is affected, not the run as a
/// whole. Upgrade path: persist a lightweight extension-method index
/// keyed by name (declaring qualname + namespace + receiver type) so a
/// later file's call sites can look up an earlier *or* later declaration —
/// that's DB-layer work (`src/db/`), out of this extractor's reach.
fn extension_method_candidates(
    method_name: &str,
    receiver_type: &ReceiverType,
    ctx: &Context,
) -> Vec<String> {
    let registry = ctx.extension_registry.borrow();
    let Some(entries) = registry.get(method_name) else {
        return Vec::new();
    };
    let mut seen = std::collections::HashSet::new();
    let mut candidates = Vec::new();
    for entry in entries {
        if !namespace_in_scope(&entry.namespace, ctx) {
            continue;
        }
        if let (ReceiverType::Known(call_ty), Some(entry_ty)) =
            (receiver_type, &entry.receiver_type)
            && call_ty != entry_ty
        {
            continue;
        }
        if seen.insert(entry.qualname.clone()) {
            candidates.push(entry.qualname.clone());
        }
    }
    candidates
}

/// Classify a declared-type (or bare constructed-type) expression's text
/// into a `LocalType`. Generic/array/tuple type shapes are never unwrapped
/// — they collapse to `Other` just like a builtin would, mirroring
/// `python::classify_annotation`'s identical ponytail simplification. A
/// trailing `?` (nullable value *or* reference type) is stripped first, so
/// `EventStore?` still resolves to `EventStore` while `int?` still
/// collapses to `Other` via the builtin check.
fn classify_annotation(text: &str) -> LocalType {
    let text = text.trim();
    let text = text.strip_suffix('?').unwrap_or(text).trim();
    if text.is_empty() {
        return LocalType::Other;
    }
    if text.contains(['<', '[', '(', ')', '{', '*']) {
        return LocalType::Other;
    }
    let bare = text.rsplit('.').next().unwrap_or(text).trim();
    classify_type_name(bare)
}

fn classify_type_name(name: &str) -> LocalType {
    if name.is_empty() || CS_BUILTIN_TYPES.contains(&name) {
        LocalType::Other
    } else {
        LocalType::Known(name.to_string())
    }
}

/// Classify a `var` local's initializer shape into a `LocalType`. The only
/// `Known` case is direct construction (`new EventStore()`); everything
/// else (a method call's return value, a collection initializer, ...) is
/// `Other` — mirrors `python::classify_assignment_value`'s identical
/// ceiling, and matches this task's "`var` only when the initializer is a
/// direct `new T()`" scope.
fn classify_value_expr(value: Node<'_>, source: &str) -> LocalType {
    if value.kind() == "object_creation_expression"
        && let Some(type_node) = value.child_by_field_name("type")
    {
        return classify_annotation(&node_text(type_node, source));
    }
    LocalType::Other
}

/// The initializer expression of a `variable_declarator`, if any — its
/// second named child (the first is always `name`); C# doesn't label this
/// with a field name of its own.
fn variable_declarator_value(node: Node<'_>) -> Option<Node<'_>> {
    let mut cursor = node.walk();
    let mut children = node.named_children(&mut cursor);
    children.next();
    children.next()
}

/// Infer types for names bound within a single method/constructor body:
/// parameters and typed/`var` local declarations. Scope is strictly this
/// method — never a caller, a callee, or another method of the same type
/// (see `Context::local_types`'s doc comment).
fn infer_local_types(function_node: Node<'_>, source: &str) -> HashMap<String, LocalType> {
    let mut bindings: Vec<(String, LocalType)> = Vec::new();
    if let Some(params) = function_node.child_by_field_name("parameters") {
        let mut cursor = params.walk();
        for param in params.named_children(&mut cursor) {
            if param.kind() != "parameter" {
                continue;
            }
            let Some(name_node) = param.child_by_field_name("name") else {
                continue;
            };
            let name = node_text(name_node, source);
            if name.is_empty() {
                continue;
            }
            let ty = param
                .child_by_field_name("type")
                .map(|t| classify_annotation(&node_text(t, source)))
                .unwrap_or(LocalType::Other);
            bindings.push((name, ty));
        }
    }
    if let Some(body) = function_node.child_by_field_name("body") {
        collect_statement_bindings(body, source, &mut bindings);
    }
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

/// Recursively collect local-variable bindings from statements within a
/// single method/constructor body, stopping at nested local-function/
/// lambda boundaries (their own locals are a different scope entirely —
/// see `Context::local_types`'s doc comment; reuses the same boundary set
/// `walk_node` itself stops at via `is_nested_function_node`).
///
/// ponytail: tuple-deconstruction targets (`var (a, b) = GetPair();`,
/// `foreach (var (k, v) in map)`) aren't tracked — the declarator/loop
/// variable's `name` field isn't a plain identifier in that shape, so it's
/// skipped rather than bound. This is no worse than before this change
/// (such names were never gated), just not improved by it.
fn collect_statement_bindings(
    node: Node<'_>,
    source: &str,
    bindings: &mut Vec<(String, LocalType)>,
) {
    if is_nested_function_node(node.kind()) {
        return;
    }
    match node.kind() {
        "variable_declaration" => {
            let type_node = node.child_by_field_name("type");
            let is_var = type_node
                .map(|t| t.kind() == "implicit_type")
                .unwrap_or(true);
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if child.kind() != "variable_declarator" {
                    continue;
                }
                let Some(name_node) = child.child_by_field_name("name") else {
                    continue;
                };
                if name_node.kind() != "identifier" {
                    continue;
                }
                let name = node_text(name_node, source);
                if name.is_empty() {
                    continue;
                }
                let ty = if is_var {
                    variable_declarator_value(child)
                        .map(|v| classify_value_expr(v, source))
                        .unwrap_or(LocalType::Other)
                } else {
                    classify_annotation(&node_text(type_node.expect("checked above"), source))
                };
                bindings.push((name, ty));
            }
        }
        "catch_declaration" => {
            if let Some(name_node) = node.child_by_field_name("name") {
                let name = node_text(name_node, source);
                if !name.is_empty() {
                    let ty = node
                        .child_by_field_name("type")
                        .map(|t| classify_annotation(&node_text(t, source)))
                        .unwrap_or(LocalType::Other);
                    bindings.push((name, ty));
                }
            }
        }
        "foreach_statement" => {
            if let Some(left) = node.child_by_field_name("left")
                && left.kind() == "identifier"
            {
                let name = node_text(left, source);
                if !name.is_empty() {
                    let ty = node
                        .child_by_field_name("type")
                        .filter(|t| t.kind() != "implicit_type")
                        .map(|t| classify_annotation(&node_text(t, source)))
                        .unwrap_or(LocalType::Other);
                    bindings.push((name, ty));
                }
            }
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_statement_bindings(child, source, bindings);
    }
}

/// Type-annotated fields (`field_declaration`) and properties
/// (`property_declaration`) declared directly in a type's body — not
/// inside any method. Used only to resolve a single-hop
/// `this.field.Method()` receiver; see `infer_receiver_type`.
fn collect_class_level_attr_types(
    class_body: Node<'_>,
    source: &str,
) -> HashMap<String, LocalType> {
    let mut result = HashMap::new();
    let mut cursor = class_body.walk();
    for member in class_body.named_children(&mut cursor) {
        match member.kind() {
            "field_declaration" => {
                let mut inner = member.walk();
                for decl in member.named_children(&mut inner) {
                    if decl.kind() != "variable_declaration" {
                        continue;
                    }
                    let Some(type_node) = decl.child_by_field_name("type") else {
                        continue;
                    };
                    // Fields can't be `var` in real C#; defensive skip.
                    if type_node.kind() == "implicit_type" {
                        continue;
                    }
                    let ty = classify_annotation(&node_text(type_node, source));
                    let mut dcursor = decl.walk();
                    for declarator in decl.named_children(&mut dcursor) {
                        if declarator.kind() != "variable_declarator" {
                            continue;
                        }
                        let Some(name_node) = declarator.child_by_field_name("name") else {
                            continue;
                        };
                        if name_node.kind() != "identifier" {
                            continue;
                        }
                        let name = node_text(name_node, source);
                        if name.is_empty() {
                            continue;
                        }
                        result.insert(name, ty.clone());
                    }
                }
            }
            "property_declaration" => {
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
                result.insert(name, classify_annotation(&node_text(type_node, source)));
            }
            _ => {}
        }
    }
    result
}

/// Type-annotated fields (`field_declaration`) and properties
/// (`property_declaration`) declared directly in a type's body whose
/// declared type itself is a corroborated gRPC client type (see
/// `split_client_service_and_prefix`) — regardless of what, if anything,
/// initializes them. A field's own initializer is very often `default!`,
/// with the client set for real in a constructor parameter instead (dpb's
/// actual shape, e.g. `public readonly TeamService.TeamServiceClient
/// Client = default!;`); the *declared type* is the only signal this needs.
/// Reuses `collect_grpc_clients_from_declaration` for the field case (same
/// `field_declaration -> variable_declaration -> variable_declarator` shape
/// a local variable statement has) rather than duplicating its
/// declared-type-first logic. Feeds `Context::grpc_clients` (in-class
/// access — `handle_type` merges this in) and `Context::grpc_client_fields`
/// (cross-file access — see that type's doc for why a field is the more
/// important half of this defect: dpb's real call sites are all
/// `scope.Client.Method()`, never same-class).
fn collect_class_level_grpc_client_fields(
    class_body: Node<'_>,
    source: &str,
) -> HashMap<String, (String, Option<String>)> {
    let mut result = HashMap::new();
    let mut cursor = class_body.walk();
    for member in class_body.named_children(&mut cursor) {
        match member.kind() {
            "field_declaration" => {
                let mut inner = member.walk();
                for decl in member.named_children(&mut inner) {
                    if decl.kind() != "variable_declaration" {
                        continue;
                    }
                    collect_grpc_clients_from_declaration(decl, source, &mut result);
                }
            }
            "property_declaration" => {
                let Some(name_node) = member.child_by_field_name("name") else {
                    continue;
                };
                let Some(type_node) = member.child_by_field_name("type") else {
                    continue;
                };
                if type_node.kind() == "implicit_type" {
                    continue;
                }
                let name = node_text(name_node, source);
                if name.is_empty() {
                    continue;
                }
                if let Some(service_and_prefix) =
                    split_client_service_and_prefix(&node_text(type_node, source))
                {
                    result.insert(name, service_and_prefix);
                }
            }
            _ => {}
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::extract::LanguageExtractor;
    use crate::indexer::http;
    use crate::indexer::proto;

    #[test]
    fn extracts_map_route_and_httpclient_call() {
        let source = r#"
var app = WebApplication.Create();
app.MapGet("/api/users/{id}", Handle);
var client = new HttpClient();
client.GetAsync("/api/users/123");
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
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
    fn extracts_mapgroup_routes() {
        let source = r#"
var app = WebApplication.Create();
var group = app.MapGroup("/api");
group.MapGet("/users/{id}", Handle);
app.MapGroup("/admin").MapPost("/users", HandlePost);
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
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
                .any(|edge| edge.target_qualname.as_deref() == Some("/admin/users"))
        );
    }

    #[test]
    fn extracts_grpc_impl_and_call() {
        // The impl class's own CLR namespace (`MyApp.Grpc`) deliberately
        // differs from the proto package (`Example.V1`, brought in scope by
        // a bare `using`) -- this is the shape every real gRPC impl in the
        // wild has, and the whole point of the regression this guards: the
        // route key must come from the `using`, never from
        // `namespace_stack`, on *both* the impl and the call side (the call
        // site here sits at file scope, outside any namespace, so a
        // namespace-derived key would previously have been empty/wrong
        // there too -- see the client-side assertions below).
        let source = r#"
using Example.V1;
using Grpc.Core;

namespace MyApp.Grpc {
  public class GreeterService : Greeter.GreeterBase {
    public override Task<HelloReply> SayHello(HelloRequest request, ServerCallContext context) {
      return Task.FromResult(new HelloReply());
    }
  }
}

var client = new Greeter.GreeterClient(channel);
client.SayHelloAsync(new HelloRequest());
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
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
            .any(|edge| edge.target_qualname.as_deref() == Some("/example.v1.greeter/sayhello")));
        assert!(
            !impls.iter().any(|edge| edge.target_qualname.as_deref()
                == Some("/myapp.grpc.greeter/sayhello")),
            "must not key the route off the impl class's own CLR namespace"
        );
        // Client-side key must land on the exact same route key the impl
        // side does -- this is the RPC_IMPL/RPC_ROUTE overlap Defect 2
        // exists to fix. `Greeter.GreeterClient`'s `Greeter.` prefix is the
        // generated-code self-reference (mirrors `Greeter.GreeterBase` on
        // the impl side), not a namespace, so it's consumed rather than
        // treated as a candidate; the two bare `using`s are what actually
        // supply the package, same as the impl side.
        assert!(
            calls
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/example.v1.greeter/sayhello")),
            "call-side key must match the impl/route key, got {:?}",
            calls.iter().map(|e| &e.target_qualname).collect::<Vec<_>>()
        );
        assert!(
            !calls
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/greeter/sayhello")),
            "must not key the call off the impl class's own CLR namespace (here, no namespace \
             at all, since the call site is at file scope)"
        );
    }

    #[test]
    fn grpc_impl_route_follows_using_alias_to_proto_package() {
        // Concrete regression case: dpb's Datasource.Grpc.DeployerServiceImpl
        // inherits `DsDeploy.DeployerService.DeployerServiceBase`, where
        // `DsDeploy` is a using-alias for the generated proto namespace. The
        // impl class itself lives in an unrelated CLR namespace.
        let source = r#"
using DsDeploy = Datasource.Deployer.V1;
using Grpc.Core;

namespace Dpb.DataMgr.Datasource.Grpc {
  internal class DeployerServiceImpl : DsDeploy.DeployerService.DeployerServiceBase {
    public override Task<DsDeploy.DeploymentResponse> Deploy(
        IAsyncStreamReader<DsDeploy.DeploymentChunk> requestStream,
        ServerCallContext context) {
      return null;
    }
  }
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let impls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_IMPL_KIND)
            .collect::<Vec<_>>();
        // An alias can only ever mean one thing, so it's the sole candidate
        // -- no ambiguity, exactly one edge.
        assert_eq!(impls.len(), 1);
        assert_eq!(
            impls[0].target_qualname.as_deref(),
            Some("/datasource.deployer.v1.deployerservice/deploy")
        );
    }

    #[test]
    fn grpc_impl_bare_base_tries_every_bare_using_as_a_package_candidate() {
        // No prefix in the base-list text at all (the common case: proto
        // namespace brought in scope by a bare `using`, not an alias).
        // Every bare `using` in the file becomes a candidate; a wrong one
        // just never matches a real RPC_ROUTE downstream, so this is safe
        // even when ambiguous.
        let source = r#"
using DataProduct.Team.V1;
using Inventory.V1;
using Grpc.Core;

namespace Dpb.DataMgr.Catalog.Grpc {
  internal class InventoryServiceImpl : InventoryService.InventoryServiceBase {
    public override Task<GetInventoryResponse> GetInventory(
        GetInventoryRequest request, ServerCallContext context) {
      return null;
    }
  }
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let impls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_IMPL_KIND)
            .collect::<Vec<_>>();
        // Three bare usings -> three distinct candidate targets.
        assert_eq!(impls.len(), 3);
        assert!(impls.iter().any(|edge| edge.target_qualname.as_deref()
            == Some("/inventory.v1.inventoryservice/getinventory")));
        assert!(
            !impls.iter().any(|edge| edge.target_qualname.as_deref()
                == Some("/dpb.datamgr.catalog.grpc.inventoryservice/getinventory")),
            "must not key the route off the impl class's own CLR namespace"
        );
    }

    #[test]
    fn grpc_call_resolves_target_typed_new_from_declared_type() {
        // C# 9 target-typed `new(...)` -- `implicit_object_creation_expression`
        // -- has no `type` node of its own (see
        // `grpc_client_from_object_creation`'s doc), so the type has to come
        // from the declaration wrapped around it instead. This is dpb's own
        // shape (e.g. `dotnet/tests/Dpb.DataMgr.Tests/Fixtures/TeamServiceScope.cs`:
        // `TeamService.TeamServiceClient client = new(channel);`). Mirrors
        // `extracts_grpc_impl_and_call`'s call-side assertions exactly, just
        // with the client constructed the way dpb's fixtures actually write
        // it, to prove the route key lands on the same target either way.
        let source = r#"
using Example.V1;

Greeter.GreeterClient client = new(channel);
client.SayHelloAsync(new HelloRequest());
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let calls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_CALL_KIND)
            .collect::<Vec<_>>();
        assert!(
            calls
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/example.v1.greeter/sayhello")),
            "a target-typed new(...) construction must resolve to the same route key an \
             explicitly-typed construction would, got {:?}",
            calls.iter().map(|e| &e.target_qualname).collect::<Vec<_>>()
        );
    }

    #[test]
    fn grpc_call_resolves_client_field_exposed_from_a_different_file() {
        // dpb's actual end-to-end shape: a test-fixture "Scope" class
        // exposes its gRPC client through a field (`Client`, itself
        // constructed elsewhere -- often via target-typed `new(...)` inside
        // the class's own static factory method, and merely assigned here
        // through a constructor parameter, never reconstructed), and every
        // real call site is in a *different* file
        // (`scope.Client.SomeRpcAsync(...)` in a `*Tests.cs` file, the field
        // declared in a `*ServiceScope.cs` fixture file) -- see
        // `GrpcClientFieldRegistry`'s doc. Calling `extract` twice on the
        // same `CSharpExtractor` mirrors a real cold reindex processing both
        // files against one shared extractor instance.
        let fixture_source = r#"
using Example.V1;
using Grpc.Net.Client;

public class GreeterScope
{
    public readonly Greeter.GreeterClient Client = default!;

    private GreeterScope(Greeter.GreeterClient client) => Client = client;

    public static GreeterScope Create(GrpcChannel channel)
    {
        Greeter.GreeterClient client = new(channel);
        return new(client);
    }
}
"#;
        let test_source = r#"
using Example.V1;

var scope = GreeterScope.Create(channel);
scope.Client.SayHelloAsync(new HelloRequest());
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        extractor
            .extract(fixture_source, "fixtures/greeter_scope")
            .unwrap();
        let file = extractor
            .extract(test_source, "tests/greeter_tests")
            .unwrap();
        let calls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_CALL_KIND)
            .collect::<Vec<_>>();
        assert!(
            calls
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/example.v1.greeter/sayhello")),
            "a gRPC client exposed through a same-named field declared in a different file must \
             still resolve, got {:?}",
            calls.iter().map(|e| &e.target_qualname).collect::<Vec<_>>()
        );
    }

    #[test]
    fn azure_style_client_type_is_not_mistaken_for_a_grpc_client() {
        // Concrete regression case: dpb's own false-positive source. Azure
        // SDK (`BlobServiceClient`, `SecretClient`, `ServiceBusClient`) and
        // Microsoft Graph SDK (`GraphServiceClient`) types are all
        // `...Client`-suffixed but never carry the mandatory
        // `{Service}.{Service}Client` self-reference stutter a real
        // generated gRPC client always has -- confirmed against dpb, every
        // one of these is constructed either fully unqualified or (rarely)
        // fully qualified without the stutter (`new
        // Azure.Storage.Blobs.BlobServiceClient(...)`), never as
        // `BlobService.BlobServiceClient`. Neither shape should register as
        // a gRPC client at all, so no RPC_CALL should ever come from using
        // one, however it's later called.
        let source = r#"
using Azure.Storage.Blobs;

var blobService = new BlobServiceClient(connectionString);
var containerClient = blobService.GetBlobContainerClient(containerId);
containerClient.CreateIfNotExistsAsync();

var fullyQualified = new Azure.Storage.Blobs.BlobServiceClient(connectionString);
fullyQualified.GetBlobContainerClient(containerId);
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let calls = file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_CALL_KIND)
            .collect::<Vec<_>>();
        assert!(
            calls.is_empty(),
            "an Azure SDK ...Client type (no gRPC self-reference stutter) must never produce an \
             RPC_CALL, got {:?}",
            calls.iter().map(|e| &e.target_qualname).collect::<Vec<_>>()
        );
    }

    /// Neuters `split_client_service_and_prefix`'s mandatory self-reference
    /// stutter requirement (see that function's doc) directly, to prove
    /// `azure_style_client_type_is_not_mistaken_for_a_grpc_client` is
    /// non-vacuous: with the corroboration check disabled, the exact same
    /// Azure SDK construction from that test *does* register (as a bogus
    /// "BlobService" client), showing the test would fail to catch a
    /// regression that reintroduced the old, unguarded behavior.
    #[test]
    fn split_client_service_and_prefix_without_stutter_check_would_match_azure_types() {
        fn split_without_stutter_requirement(text: &str) -> Option<(String, Option<String>)> {
            let text = text.trim();
            if text.is_empty() {
                return None;
            }
            let mut parts: Vec<&str> = text.split('.').map(str::trim).collect();
            let last = parts.pop()?;
            let last = last.split('<').next().unwrap_or(last).trim();
            let service = last.strip_suffix("Client")?;
            if service.is_empty() {
                return None;
            }
            Some((service.to_string(), None))
        }
        assert_eq!(
            split_without_stutter_requirement("BlobServiceClient"),
            Some(("BlobService".to_string(), None))
        );
        // The real (fixed) function must reject the same input.
        assert_eq!(split_client_service_and_prefix("BlobServiceClient"), None);
    }

    #[test]
    fn is_likely_interface_name_detects_i_prefix() {
        assert!(is_likely_interface_name("IKeyVaultCredentialStore"));
        assert!(is_likely_interface_name("IOptions"));
        assert!(is_likely_interface_name("Foo.Bar.IDisposable"));
        assert!(is_likely_interface_name("IEnumerable<T>"));
        assert!(!is_likely_interface_name("BaseClass"));
        assert!(!is_likely_interface_name("Integer"));
        // "I" alone or "Iota" (lowercase after I) are not interfaces
        assert!(!is_likely_interface_name("I"));
    }

    #[test]
    fn class_implementing_interface_gets_implements_edge() {
        let source = r#"
namespace Acme;
public class MyService : IMyService {
    public void DoWork() {}
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let implements = file
            .edges
            .iter()
            .filter(|e| e.kind == "IMPLEMENTS")
            .collect::<Vec<_>>();
        let extends = file
            .edges
            .iter()
            .filter(|e| e.kind == "EXTENDS")
            .collect::<Vec<_>>();
        assert!(
            implements
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("IMyService")),
            "expected IMPLEMENTS edge to IMyService"
        );
        assert!(
            extends.is_empty(),
            "expected no EXTENDS edges, found: {:?}",
            extends
                .iter()
                .map(|e| e.target_qualname.as_deref())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn class_extending_class_then_interface() {
        let source = r#"
namespace Acme;
public class MyService : BaseService, IMyService {
    public void DoWork() {}
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let extends = file
            .edges
            .iter()
            .filter(|e| e.kind == "EXTENDS")
            .collect::<Vec<_>>();
        let implements = file
            .edges
            .iter()
            .filter(|e| e.kind == "IMPLEMENTS")
            .collect::<Vec<_>>();
        assert!(
            extends
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("BaseService")),
            "expected EXTENDS edge to BaseService"
        );
        assert!(
            implements
                .iter()
                .any(|e| e.target_qualname.as_deref() == Some("IMyService")),
            "expected IMPLEMENTS edge to IMyService"
        );
    }

    #[test]
    fn extract_generic_type_arg_simple() {
        assert_eq!(
            extract_generic_type_arg("Configure<DatabaseOptions>"),
            Some("DatabaseOptions".to_string())
        );
    }

    #[test]
    fn extract_generic_type_arg_nested() {
        assert_eq!(
            extract_generic_type_arg("GetRequiredService<IOptions<DatabaseOptions>>"),
            Some("IOptions<DatabaseOptions>".to_string())
        );
    }

    #[test]
    fn extract_options_type_variants() {
        assert_eq!(
            extract_options_type("IOptions<DatabaseOptions>"),
            Some("DatabaseOptions".to_string())
        );
        assert_eq!(
            extract_options_type("IOptionsMonitor<LoggingOptions>"),
            Some("LoggingOptions".to_string())
        );
        assert_eq!(extract_options_type("ILogger<Foo>"), None);
    }

    #[test]
    fn extract_di_options_from_mixed_params() {
        let result = extract_di_options_types_from_params(
            "(IOptions<DatabaseOptions> db, ILogger<Foo> logger, IOptionsMonitor<CacheOptions> cache)",
        );
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].0, "DatabaseOptions");
        assert_eq!(result[0].1, "IOptions");
        assert_eq!(result[1].0, "CacheOptions");
        assert_eq!(result[1].1, "IOptionsMonitor");
    }
}
