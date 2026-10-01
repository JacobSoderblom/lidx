use crate::db::resolver::{
    Declaration, DeclarationIndex, DeclarationQuery, LanguageProfile, RpcCallEdge, ScopeImports,
    VisibilityRule,
};
use crate::indexer::channel;
use crate::indexer::config;
use crate::indexer::extract::{
    CallShape, DeclIdentity, DeferredArgument, DeferredBase, DeferredMarker, DeferredReturn,
    EdgeInput, ExtractedFile, MAX_DEFERRED_DEPTH, ReceiverType, SymbolInput, TypeScope,
    pinning_new_edges,
};
use crate::indexer::http;
use crate::indexer::proto;
use crate::indexer::scan;
use crate::indexer::string_consts::{
    LocalBinding, StringConsts, csharp_declarator_initializer, scan_enclosing_function,
};
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

/// C#'s resolution profile: the shared default (dot-separated, no
/// relative-import rewriting, import-tier miss refuses the name tiers,
/// suffix matching on), plus a recorded-visibility rule — `handle_method`
/// records `visibility = "private"` for an explicit `private` modifier
/// (see `has_modifier`), which the guarded name-fallback tier then refuses
/// to bind across files.
pub(crate) const PROFILE: LanguageProfile = LanguageProfile {
    visibility: VisibilityRule::Recorded,
    deferred_receiver: Some(resolve_deferred),
    deferred_rpc: Some(deferred_rpc_calls),
    ..LanguageProfile::DEFAULT
};

#[derive(Clone)]
struct Context {
    /// Same-file string constants (see `string_consts`), used to resolve
    /// channel topics given as identifiers.
    string_consts: Rc<StringConsts>,
    module: String,
    namespace_stack: Vec<String>,
    type_stack: Vec<String>,
    /// Generic arity of each enclosing type, outermost first (part of a
    /// symbol's identity: `Box<T>` and `Box<T, U>` are different types).
    generic_arities: Vec<usize>,
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
    /// Plain assignments (`x = M()`) to a local or field within the current
    /// method body; see `type_at`.
    assigns: Rc<ScopeAssigns>,
    /// Type-annotated fields and properties of the *directly* enclosing
    /// type, read once when entering its body — see
    /// `collect_class_level_attr_types`. Used only to resolve a single-hop
    /// `this.field.Method()` receiver.
    class_attr_types: Rc<HashMap<String, LocalType>>,
    /// Declared type *text* of the same fields/properties (only used to find
    /// a `foreach` deconstruction's element types).
    class_attr_raw: Rc<HashMap<String, String>>,
    /// The directly enclosing type's own base class, if its `base_list`
    /// names one and it's resolvable (see `handle_type`) — used only to
    /// resolve a `base.Method()` receiver. `Other` when the type has no
    /// base class, only implements interfaces, or the first base-list
    /// entry isn't cheaply classifiable.
    base_type: LocalType,
    /// The enclosing class's first base type, generic arguments stripped: what `: base(..)` constructs, known
    /// even when the type is generic or lives in another file.
    base_class_name: Option<String>,
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
    /// Declared return-type text of every method / local function in *this
    /// file* whose bare name is unambiguous (declared once, or several
    /// times with the identical return type) — see
    /// `collect_method_return_types`. Feeds `var x = Method(..)` /
    /// `var (a, b) = Method(..)` inference. Same-file only; a callee
    /// declared elsewhere stays untracked.
    method_returns: Rc<MethodReturns>,
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
/// that name exists anywhere in the repo — see
/// `collect_class_level_grpc_client_fields`, `prescan_grpc_client_fields`.
///
/// Deliberately *not* built incrementally as `extract()` processes each
/// file (an earlier version of this did exactly that, mirroring
/// `ExtensionRegistry`'s design, and was wrong: whether a call site
/// resolves ended up depending on directory sort order — dpb's own
/// `Dpb.DataMgr.Tests/DataProduct` sorts before `.../Fixtures`, so
/// `TeamServiceTests.cs` was extracted while the registry was still empty
/// and silently lost every edge, while `SourcingIntegrationTests.cs` two
/// directories over, whose `Fixtures` happens to sort first, resolved
/// fine — same source shape, opposite outcome, decided purely by scan
/// order). A generated gRPC client is very often exposed through a
/// same-named field on a small, repeated test-fixture shape (dpb's own
/// corpus: eight distinct generated clients, all exposed as a field
/// literally named `Client`, in a different file than every one of their
/// call sites), so silently depending on scan order isn't an acceptable
/// trade-off here the way it is for `ExtensionRegistry` (a real cross-file
/// symbol table doesn't exist for this single-file extractor otherwise, so
/// that one's ponytail-documented order dependence is accepted as a
/// narrower, rarer miss — this one was the single most common real-world
/// shape).
///
/// Instead this is fully populated by a one-time, whole-repo prescan
/// (`prescan_grpc_client_fields`) before any call site's cross-file
/// resolution is attempted — see `CSharpExtractor::grpc_prescan_done` and
/// `resolve_imports`. `extract()` itself never reads or writes this
/// directly any more; a call site that can't resolve locally
/// (`grpc_service_from_client_binding`, same-file only) instead emits a
/// `PENDING_GRPC_CLIENT_CALL_KIND` placeholder edge
/// (`pending_grpc_client_call_edge`) that `resolve_pending_grpc_calls`
/// replaces with the real `RPC_CALL` edge(s) once this registry is known
/// to be complete, regardless of which file was extracted first.
///
/// A lookup still fans out over every candidate rather than picking one —
/// the receiver's own declaring type is invisible from here, so there's no
/// way to disambiguate — exactly the same "a wrong candidate simply never
/// matches a real route downstream" tolerance `grpc_impl_edge`/
/// `grpc_call_edge` already rely on for candidate *packages*. Every entry
/// here already passed `split_client_service_and_prefix`'s mandatory
/// self-reference check before being admitted, so this can only ever fan
/// out over genuine generated-code candidates, never arbitrary
/// `...Client`-suffixed types.
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
    /// Transient: `var x = recv.Method(..)` before `resolve_pending_calls`
    /// has looked `recv` up. Never present in a finished `local_types` map.
    Call(PendingCall),
    /// The return value of a callee declared in another file; the resolver
    /// finishes it (see `ReceiverType::Deferred`). Holds the column text.
    Deferred(DeferredReturn),
}

/// `recv.Method(..)` awaiting `resolve_pending_calls`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingCall {
    /// Where the call starts, for `type_at` lookups of its receiver.
    pos: usize,
    recv: PendingRecv,
    method: String,
    awaited: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PendingRecv {
    /// A bare identifier (a local, field, or static type name) or
    /// `this.field`.
    Name(String),
    /// The enclosing type: `M()` / `this.M()`.
    This,
    /// `base.M()`.
    Base,
    /// The value of another call (`a.B().C()`).
    Call(Box<PendingCall>),
}

/// The enclosing type (`Foo` for a bare / `this.` call, possibly inherited
/// or declared in another partial-class file) and its base class.
#[derive(Default)]
struct ThisEnv {
    this_type: Option<String>,
    base_type: Option<String>,
}

impl ThisEnv {
    fn from_ctx(ctx: &Context) -> Self {
        Self {
            this_type: ctx.type_stack.last().cloned(),
            base_type: match &ctx.base_type {
                LocalType::Known(t) => Some(t.clone()),
                _ => None,
            },
        }
    }
}

impl LocalType {
    fn receiver(&self) -> ReceiverType {
        match self {
            LocalType::Known(ty) => ReceiverType::Known(ty.clone()),
            LocalType::Deferred(marker) => ReceiverType::Deferred(marker.clone()),
            _ => ReceiverType::Unresolved,
        }
    }
}

pub struct CSharpExtractor {
    parser: Parser,
    /// Accumulates across every file this extractor instance processes —
    /// see `Context::extension_registry`'s doc.
    extension_registry: ExtensionRegistry,
    /// Populated exactly once, by `prescan_grpc_client_fields` — see
    /// `GrpcClientFieldRegistry`'s doc.
    grpc_client_fields: GrpcClientFieldRegistry,
    /// Guards `prescan_grpc_client_fields`, which re-parses every `.cs`
    /// file in the repo and so is relatively expensive: `false` until the
    /// first `resolve_imports` call runs it, `true` from then on so every
    /// later call just reuses the now-complete `grpc_client_fields`. A
    /// `Cell` rather than storing the result directly because
    /// `resolve_imports` only gets `&self`.
    grpc_prescan_done: std::cell::Cell<bool>,
    /// `global using` directives of the file's whole project (see
    /// `cs_globals`), applied to every file's imports. Set per file by
    /// `set_project_globals`.
    project_globals: Vec<String>,
}

/// The `global using` directives in `source`, one entry per directive: the
/// namespace, or `Alias=Target` for an alias. Line-based (a cheap pre-pass
/// over changed files); `global using static` is skipped like `using static`.
pub fn scan_global_usings(source: &str) -> Vec<String> {
    let mut out = Vec::new();
    for line in source.lines() {
        let Some(rest) = line.trim_start().strip_prefix("global") else {
            continue;
        };
        let Some(rest) = rest.trim_start().strip_prefix("using") else {
            continue;
        };
        if !rest.starts_with(char::is_whitespace) {
            continue;
        }
        let rest = rest.split("//").next().unwrap_or(rest);
        let rest = rest.split(';').next().unwrap_or(rest).trim();
        if rest.is_empty() || rest.starts_with("static ") {
            continue;
        }
        let entry: String = match rest.split_once('=') {
            Some((alias, target)) => format!("{}={}", alias.trim(), target.trim()),
            None => rest.to_string(),
        };
        if !out.contains(&entry) {
            out.push(entry);
        }
    }
    out
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
            grpc_prescan_done: std::cell::Cell::new(false),
            project_globals: Vec::new(),
        })
    }
}

impl crate::indexer::extract::LanguageExtractor for CSharpExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String {
        module_name_from_rel_path(rel_path)
    }

    fn set_project_globals(&mut self, globals: &[String]) {
        self.project_globals = globals.to_vec();
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
            string_consts: Rc::new(crate::indexer::string_consts::collect_string_consts(
                crate::indexer::string_consts::ConstLang::CSharp,
                root,
                source,
            )),
            module: module_name.to_string(),
            namespace_stack: Vec::new(),
            type_stack: Vec::new(),
            generic_arities: Vec::new(),
            fn_depth: 0,
            current_scope: module_name.to_string(),
            route_prefix: None,
            route_groups: HashMap::new(),
            grpc_service: None,
            grpc_clients: HashMap::new(),
            grpc_package_candidates: Vec::new(),
            // ponytail: unlike Python/TypeScript, there's no meaningful
            // module-top-level scope in C# (locals only ever live inside a
            // method/constructor body), so this starts and stays empty
            // outside of `handle_method`/`handle_constructor`.
            local_types: Rc::new(HashMap::new()),
            assigns: Rc::new(ScopeAssigns::default()),
            class_attr_types: Rc::new(HashMap::new()),
            class_attr_raw: Rc::new(HashMap::new()),
            base_type: LocalType::Other,
            base_class_name: None,
            imports: Rc::new({
                let mut imports = collect_import_context(root, source);
                imports.apply_globals(&self.project_globals);
                imports
            }),
            extension_registry: Rc::clone(&self.extension_registry),
            method_returns: Rc::new(MethodReturns::collect(root, source)),
        };
        if root.kind() == "compilation_unit" {
            walk_compilation_unit(root, &ctx, source, &mut output);
        } else {
            walk_node(root, &ctx, source, &mut output);
        }
        // A qualname shared by overloads counts as static only when every
        // one of them is.
        let statics = std::mem::take(&mut output.static_member_qualnames);
        for q in &statics {
            let declared = output
                .symbols
                .iter()
                .filter(|s| matches!(s.kind.as_str(), "method" | "field") && s.qualname == *q)
                .count();
            if statics.iter().filter(|o| *o == q).count() == declared
                && !output.static_member_qualnames.contains(q)
            {
                output.static_member_qualnames.push(q.clone());
            }
        }
        Ok(output)
    }

    /// Finishes every `PENDING_GRPC_CLIENT_CALL_KIND` placeholder `extract()`
    /// left in `edges` (see that constant's doc) into real `RPC_CALL` edges
    /// — the only `LanguageExtractor` hook that receives `repo_root`, so
    /// the only place `prescan_grpc_client_fields` can run from. Runs the
    /// prescan itself at most once per `CSharpExtractor` instance (i.e.
    /// once per reindex), on whichever C# file's `resolve_imports` call
    /// happens to come first — see `grpc_prescan_done`.
    fn resolve_imports(
        &self,
        repo_root: &Path,
        _file_rel_path: &str,
        _module_name: &str,
        edges: &mut Vec<EdgeInput>,
    ) {
        if !self.grpc_prescan_done.get() {
            prescan_grpc_client_fields(repo_root, &self.grpc_client_fields);
            self.grpc_prescan_done.set(true);
        }
        resolve_pending_grpc_calls(edges, &self.grpc_client_fields);
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
            next_ctx.namespace_stack = parts;
            next_ctx.current_scope = qualname;
        }
    }
    next_ctx.route_groups = collect_global_route_groups(node, source);
    next_ctx.grpc_clients = collect_global_grpc_clients(node, source, &ctx.method_returns);
    let (local_types, assigns) = infer_global_local_types(node, source, ctx);
    next_ctx.local_types = Rc::new(local_types);
    next_ctx.assigns = Rc::new(assigns);

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "file_scoped_namespace_declaration" {
            continue;
        }
        walk_node(child, &next_ctx, source, output);
    }
}

/// Number of type parameters a type or method declares (`Box<T, U>` is 2).
fn generic_arity(node: Node<'_>) -> usize {
    let mut cursor = node.walk();
    let list = node.child_by_field_name("type_parameters").or_else(|| {
        node.named_children(&mut cursor)
            .find(|c| c.kind() == "type_parameter_list")
    });
    let Some(list) = list else {
        return 0;
    };
    let mut inner = list.walk();
    list.named_children(&mut inner)
        .filter(|c| c.kind() == "type_parameter")
        .count()
}

/// Declaration identity: the generic arity of every enclosing type plus, for
/// a type or method, its own. `None` when nothing is generic, so ordinary
/// symbols keep their ids (issue #212).
fn identity(ctx: &Context, own: Option<usize>) -> Option<DeclIdentity> {
    let arities: Vec<usize> = ctx.generic_arities.iter().copied().chain(own).collect();
    arities.iter().any(|a| *a > 0).then(|| DeclIdentity {
        generic_arities: arities,
        ..DeclIdentity::default()
    })
}

fn walk_node(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    // Same-qualname declarations (`Box<T>` / `Box<T, U>`, issue #212) each
    // keep their own outgoing and CONTAINS edges. Wrapping every node
    // (rather than listing declaration kinds) means a new declaration kind
    // is covered without touching this; a node that emits no symbol costs
    // two length reads.
    pinning_new_edges(output, |output| walk_node_inner(node, ctx, source, output));
}

fn walk_node_inner(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if matches!(
        node.kind(),
        "invocation_expression"
            | "object_creation_expression"
            | "implicit_object_creation_expression"
    ) {
        handle_call(node, ctx, source, output);
    }
    // Configuration["KEY"] — element_access_expression
    if node.kind() == "element_access_expression"
        && let Some(edge) = config_indexer_read_edge(node, ctx, source)
    {
        output.edges.push(edge);
    }
    if node.kind() == "member_access_expression" {
        handle_member_read(node, ctx, source, output);
    }
    if is_local_function_node(node.kind()) {
        return;
    }
    // A lambda/anonymous-method body is a nested *scope*, not a new symbol
    // — fall through into the generic recursion below with the same `ctx`
    // so calls inside it (e.g. `_connection.EnsureOpenAsync()` inside a
    // Polly pipeline callback) attribute to the enclosing named symbol via
    // `ctx.current_scope`, instead of being silently dropped. None of the
    // match arms below fire for a lambda-body node kind, so no special case
    // is needed here beyond not returning early.
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
        "enum_member_declaration" => {
            handle_enum_member(node, ctx, source, output);
            return;
        }
        "indexer_declaration"
        | "operator_declaration"
        | "conversion_operator_declaration"
        | "destructor_declaration" => {
            handle_special_member(node, ctx, source, output);
            return;
        }
        "delegate_declaration" => {
            handle_delegate(node, ctx, source, output);
            return;
        }
        "event_declaration" => {
            handle_event(node, ctx, source, output);
            return;
        }
        "event_field_declaration" => {
            handle_event_field(node, ctx, source, output);
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

/// `Type.Member` read (not a call): a `USES` edge whose target the resolver
/// binds through the file's `using`s, so an enum member read from another
/// file resolves to the member symbol. Limited to a bare PascalCase type
/// name and member that aren't a tracked local/field.
fn handle_member_read(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    if ctx.current_scope.is_empty() {
        return;
    }
    let (Some(object), Some(member)) = (
        node.child_by_field_name("expression"),
        node.child_by_field_name("name"),
    ) else {
        return;
    };
    if object.kind() != "identifier" || member.kind() != "identifier" {
        return;
    }
    if let Some(parent) = node.parent()
        && parent.kind() == "invocation_expression"
        && parent.child_by_field_name("function") == Some(node)
    {
        return;
    }
    let (ty, name) = (node_text(object, source), node_text(member, source));
    if !ty.starts_with(char::is_uppercase) || !name.starts_with(char::is_uppercase) {
        return;
    }
    if is_qualified_name_prefix(node) || is_inside_nameof(node, source) {
        return;
    }
    let receiver_type = infer_receiver_type(node, source, ctx);
    if receiver_type != ReceiverType::NotTracked || ctx.local_types.contains_key(ty.as_str()) {
        return;
    }
    let (start_line, _, end_line, _, start_byte, end_byte) = span(node);
    output.edges.push(EdgeInput {
        kind: "USES".to_string(),
        source_qualname: Some(ctx.current_scope.clone()),
        target_qualname: Some(format!("{ty}.{name}")),
        evidence_snippet: util::edge_evidence_snippet(
            source, start_byte, end_byte, start_line, end_line,
        ),
        import_candidates: import_qualified_candidates(&ty, &name, ctx),
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    });
}

/// `System.Console` in `System.Console.Out`: the receiver of a further
/// un-invoked PascalCase member read is a qualified name, not a `Type.Member`
/// read. (`Color.Red.ToString()` is invoked, so `Color.Red` still counts.)
fn is_qualified_name_prefix(node: Node<'_>) -> bool {
    let Some(parent) = node.parent() else {
        return false;
    };
    if parent.kind() != "member_access_expression"
        || parent.child_by_field_name("expression") != Some(node)
    {
        return false;
    }
    let invoked = parent.parent().is_some_and(|gp| {
        gp.kind() == "invocation_expression" && gp.child_by_field_name("function") == Some(parent)
    });
    !invoked
}

/// Inside `nameof(...)`, where `Type.Member` is a name, not a read.
fn is_inside_nameof(node: Node<'_>, source: &str) -> bool {
    let mut cur = node.parent();
    while let Some(n) = cur {
        if n.kind() == "invocation_expression"
            && n.child_by_field_name("function")
                .is_some_and(|f| f.kind() == "identifier" && node_text(f, source) == "nameof")
        {
            return true;
        }
        cur = n.parent();
    }
    false
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
        identity: None,
    });
    // The file module and a namespace can share a qualname (`Shop.cs` with
    // `namespace Shop`); that would be a `CONTAINS` edge from a symbol to itself.
    let container = container_qualname(ctx);
    if container != qualname {
        output.edges.push(EdgeInput {
            kind: "CONTAINS".to_string(),
            source_qualname: Some(container),
            target_qualname: Some(qualname.clone()),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
    }

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
        identity: identity(ctx, Some(generic_arity(node))),
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
        handle_base_list(node, &qualname, source, output, type_kind, ctx);
    }
    if type_kind == TypeKind::Record {
        let mut member_ctx = ctx.clone();
        member_ctx.type_stack.push(name.clone());
        member_ctx.generic_arities.push(generic_arity(node));
        handle_record_parameters(node, &member_ctx, source, output);
    }

    let grpc_service_info = grpc_service_from_bases(node, source);
    let grpc_package_candidates = grpc_service_info
        .as_ref()
        .map(|(_, prefix)| {
            grpc_package_candidates_from_prefix(
                prefix.as_deref(),
                &ctx.imports.aliases,
                &ctx.imports.namespaces,
            )
        })
        .unwrap_or_default();
    let class_prefix = route_prefix_from_attributes(node, source);
    let combined_prefix =
        combine_route_prefix(ctx.route_prefix.as_deref(), class_prefix.as_deref());
    let mut next_ctx = ctx.clone();
    next_ctx.type_stack.push(name);
    next_ctx.generic_arities.push(generic_arity(node));
    next_ctx.current_scope = qualname;
    next_ctx.route_prefix = combined_prefix;
    next_ctx.grpc_service = grpc_service_info.map(|(service, _)| service);
    next_ctx.grpc_package_candidates = grpc_package_candidates;
    next_ctx.base_type = resolvable_base_type(node, source, type_kind);
    next_ctx.base_class_name = base_class_name(node, source, type_kind);
    // Top-level-statement locals aren't visible inside a type.
    next_ctx.local_types = Rc::new(HashMap::new());
    next_ctx.assigns = Rc::new(ScopeAssigns::default());
    if let Some(body) = node.child_by_field_name("body") {
        next_ctx.class_attr_types = Rc::new(collect_class_level_attr_types(body, source));
        next_ctx.class_attr_raw = Rc::new(collect_class_level_attr_type_texts(body, source));
        // In-class access only (`Client.Method()`/`this.Client.Method()`
        // from within this same type) — same-file, so this is fine to
        // resolve directly here, unlike the cross-file case
        // `GrpcClientFieldRegistry`/`prescan_grpc_client_fields` exists
        // for (see that type's doc for why this one *can't* be resolved
        // here: the registry may still be incomplete at this point,
        // depending on scan order).
        let grpc_fields = collect_class_level_grpc_client_fields(body, source, &ctx.method_returns);
        if !grpc_fields.is_empty() {
            let mut clients = next_ctx.grpc_clients.clone();
            for (field_name, service_and_prefix) in grpc_fields {
                clients.entry(field_name).or_insert(service_and_prefix);
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

fn base_class_name(node: Node<'_>, source: &str, type_kind: TypeKind) -> Option<String> {
    if !matches!(type_kind, TypeKind::Class | TypeKind::Record) {
        return None;
    }
    let mut cursor = node.walk();
    let list = node
        .named_children(&mut cursor)
        .find(|c| c.kind() == "base_list")?;
    let first = base_list_types(list, source).into_iter().next()?;
    let name = strip_type_args(&first);
    // An interface-looking first entry is kept: `IISManager` may be a class,
    // and the resolver refuses a target that turns out to be an interface.
    (!name.is_empty()).then_some(name)
}

fn handle_method(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    // Explicit interface implementation (`void IA.Run()`): a distinct symbol
    // `C.IA.Run` (name `Run`) so it never collides with an implicit `C.Run`.
    let qualname = match explicit_interface_name(node, source) {
        Some(iface) => build_qualname(ctx, &format!("{iface}.{name}")),
        None => build_qualname(ctx, &name),
    };
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = method_signature(node, source);
    if has_modifier(node, source, "private") {
        output.private_qualnames.push(qualname.clone());
    }
    if has_modifier(node, source, "static") {
        output.static_member_qualnames.push(qualname.clone());
    }
    if has_modifier(node, source, "override") {
        output.override_symbols.push((qualname.clone(), start_line));
    }
    let first_edge = output.edges.len();
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
        identity: identity(ctx, Some(generic_arity(node))),
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
    walk_parameter_defaults(node, &qualname, ctx, source, output);
    if let Some(body) = node.child_by_field_name("body") {
        let mut next_ctx = ctx.clone();
        next_ctx.fn_depth += 1;
        next_ctx.current_scope = qualname.clone();
        next_ctx.route_groups = collect_route_groups(body, source);
        let mut grpc_clients = ctx.grpc_clients.clone();
        grpc_clients.extend(collect_grpc_clients(body, source, &ctx.method_returns));
        next_ctx.grpc_clients = grpc_clients;
        let (local_types, assigns) = infer_local_types(node, source, ctx);
        next_ctx.local_types = Rc::new(local_types);
        next_ctx.assigns = Rc::new(assigns);
        walk_node(body, &next_ctx, source, output);
    }
    pin_edge_sources(&mut output.edges[first_edge..], &qualname, start_byte);
}

/// Walk the default-value expressions of a method/constructor's parameters
/// (`Widget w = new()`), attributing their calls to the method itself.
fn walk_parameter_defaults(
    node: Node<'_>,
    scope: &str,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let Some(params) = node.child_by_field_name("parameters") else {
        return;
    };
    let mut next_ctx = ctx.clone();
    next_ctx.current_scope = scope.to_string();
    let mut cursor = params.walk();
    for param in params.named_children(&mut cursor) {
        if param.kind() != "parameter" {
            continue;
        }
        let skipped: Vec<_> = ["type", "name"]
            .iter()
            .filter_map(|f| param.child_by_field_name(f))
            .collect();
        let mut inner = param.walk();
        for child in param.named_children(&mut inner) {
            if !skipped.contains(&child) && child.kind() != "attribute_list" {
                walk_node(child, &next_ctx, source, output);
            }
        }
    }
}

/// Pin the edges emitted from (and the CONTAINS edge to) the overload at
/// `start_byte` to it: several
/// C# overloads share one qualname, so the qualname alone can't say which
/// symbol an edge belongs to.
fn pin_edge_sources(edges: &mut [EdgeInput], qualname: &str, start_byte: i64) {
    for edge in edges {
        if edge.source_qualname.as_deref() == Some(qualname) {
            edge.source_start_byte = Some(start_byte);
        }
        if edge.kind == "CONTAINS" && edge.target_qualname.as_deref() == Some(qualname) {
            edge.target_start_byte = Some(start_byte);
        }
    }
}

/// Identity segment of an `explicit_interface_specifier` (`void N1.IA<int>.Run()`
/// -> `N1.IA<int>`), or `None` for an ordinary member. The member's qualname
/// is `Class.<identity>.Name`, so closed generics and same-named interfaces
/// from different namespaces never collide.
fn explicit_interface_name(node: Node<'_>, source: &str) -> Option<String> {
    let mut cursor = node.walk();
    let spec = node
        .children(&mut cursor)
        .find(|c| c.kind() == "explicit_interface_specifier")?;
    let identity = explicit_interface_identity(&node_text(spec, source))?;
    Some(strip_open_args(identity, node, source))
}

/// Type-parameter names declared by `node` or any enclosing declaration.
fn enclosing_type_params(node: Node<'_>, source: &str) -> Vec<String> {
    let mut params = Vec::new();
    let mut current = Some(node);
    while let Some(n) = current {
        let mut cursor = n.walk();
        for child in n.children(&mut cursor) {
            if child.kind() != "type_parameter_list" {
                continue;
            }
            let mut inner = child.walk();
            for tp in child.named_children(&mut inner) {
                if tp.kind() == "type_parameter" {
                    params.push(
                        node_text(tp, source)
                            .trim_start_matches(|c: char| !c.is_alphanumeric() && c != '_')
                            .to_string(),
                    );
                }
            }
        }
        current = n.parent();
    }
    params
}

/// `IA<T>` where `T` is a type parameter in scope at `node` is an *open*
/// interface: drop the arguments (keeping the qualifier) so it pairs with
/// every closed impl instead of a bogus `<T>` one.
fn strip_open_args(identity: String, node: Node<'_>, source: &str) -> String {
    let Some(open) = identity.find('<') else {
        return identity;
    };
    let params = enclosing_type_params(node, source);
    let mentions_param = identity[open..]
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|tok| params.iter().any(|p| p == tok));
    if mentions_param {
        identity[..open].to_string()
    } else {
        identity
    }
}

/// `global::N.Outer<T>.IA<Dictionary<K, V>>.` -> `N.Outer.IA<Dictionary<K,V>>`:
/// whitespace and `global::` dropped, generic arguments kept only on the
/// last segment (the interface itself), so the text before the first `<`
/// is the interface's namespace-qualified name as written.
fn explicit_interface_identity(text: &str) -> Option<String> {
    let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
    let compact = compact.trim_end_matches('.');
    let compact = compact.strip_prefix("global::").unwrap_or(compact);
    let mut depth = 0usize;
    let mut last_dot = None;
    for (i, ch) in compact.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => depth = depth.saturating_sub(1),
            '.' if depth == 0 => last_dot = Some(i),
            _ => {}
        }
    }
    let (head, last) = match last_dot {
        Some(i) => (&compact[..i], &compact[i + 1..]),
        None => ("", compact),
    };
    if last.is_empty() {
        return None;
    }
    let last = match (last.find('<'), last.rfind('>')) {
        (Some(open), Some(close)) if close > open => format!(
            "{}<{}>",
            &last[..open],
            normalize_type_args(&last[open + 1..close])
        ),
        _ => last.to_string(),
    };
    if head.is_empty() {
        Some(last)
    } else {
        Some(format!("{}.{last}", strip_type_args(head)))
    }
}

/// One canonical spelling of a generic argument list, so `int`/`Int32`,
/// `System.String`/`string`/`string?`, spacing and nested generics compare
/// equal (`Dictionary<string, List<Int32>>` -> `Dictionary<string,List<int>>`).
/// A `?` is kept only where it means `Nullable<T>` (value types); on the
/// reference types `string`/`object` it is an annotation and is dropped.
fn normalize_type_args(text: &str) -> String {
    const ALIASES: &[(&str, &str)] = &[
        ("SByte", "sbyte"),
        ("Byte", "byte"),
        ("Int16", "short"),
        ("UInt16", "ushort"),
        ("Int32", "int"),
        ("UInt32", "uint"),
        ("Int64", "long"),
        ("UInt64", "ulong"),
        ("Single", "float"),
        ("Double", "double"),
        ("Decimal", "decimal"),
        ("Boolean", "bool"),
        ("Char", "char"),
        ("String", "string"),
        ("Object", "object"),
    ];
    let compact: Vec<char> = text.chars().filter(|c| !c.is_whitespace()).collect();
    let mut out = String::with_capacity(compact.len());
    let mut i = 0;
    while i < compact.len() {
        if !(compact[i].is_alphanumeric() || compact[i] == '_' || compact[i] == '.') {
            out.push(compact[i]);
            i += 1;
            continue;
        }
        let start = i;
        while i < compact.len()
            && (compact[i].is_alphanumeric() || compact[i] == '_' || compact[i] == '.')
        {
            i += 1;
        }
        let token: String = compact[start..i].iter().collect();
        let token = token.strip_prefix("global::").unwrap_or(&token);
        let bare = token.strip_prefix("System.").unwrap_or(token);
        let name = ALIASES
            .iter()
            .find(|(long, _)| *long == bare)
            .map_or(token, |(_, short)| *short);
        out.push_str(name);
        if matches!(name, "string" | "object") && compact.get(i) == Some(&'?') {
            i += 1;
        }
    }
    out
}

fn handle_constructor(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    // A static constructor is a distinct symbol (`T..cctor`): `new T(..)`
    // never runs it, so it must not share the instance constructors' name.
    let name = if has_modifier(node, source, "static") {
        ".cctor"
    } else {
        ".ctor"
    };
    let qualname = build_qualname(ctx, name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = method_signature(node, source);
    let first_edge = output.edges.len();
    output.symbols.push(SymbolInput {
        kind: "method".to_string(),
        name: name.to_string(),
        qualname: qualname.clone(),
        start_line,
        start_col,
        end_line,
        end_col,
        start_byte,
        end_byte,
        signature: signature.clone(),
        docstring: None,
        identity: identity(ctx, None),
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

    let mut next_ctx = ctx.clone();
    next_ctx.fn_depth += 1;
    next_ctx.current_scope = qualname.clone();
    let (local_types, assigns) = infer_local_types(node, source, ctx);
    next_ctx.local_types = Rc::new(local_types);
    next_ctx.assigns = Rc::new(assigns);
    walk_parameter_defaults(node, &qualname, ctx, source, output);
    let mut cursor = node.walk();
    if let Some(init) = node
        .named_children(&mut cursor)
        .find(|c| c.kind() == "constructor_initializer")
    {
        output
            .edges
            .extend(constructor_initializer_edge(init, &qualname, ctx, source));
        walk_node(init, &next_ctx, source, output);
    }
    if let Some(body) = node.child_by_field_name("body") {
        walk_node(body, &next_ctx, source, output);
    }
    pin_edge_sources(&mut output.edges[first_edge..], &qualname, start_byte);
}

/// `enum Color { Red, Green = 2 }`: each member is a `const` of its enum.
fn handle_enum_member(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(ctx, &name);
    push_member(
        node,
        ctx,
        output,
        "const",
        name,
        qualname.clone(),
        None,
        None,
    );
    walk_initializer(node, &qualname, ctx, source, output);
}

/// `record P(string Name, int Age)`: each positional parameter declares a
/// `property` of the record.
fn handle_record_parameters(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let mut cursor = node.walk();
    let Some(list) = node
        .named_children(&mut cursor)
        .find(|c| c.kind() == "parameter_list")
    else {
        return;
    };
    let mut inner = list.walk();
    for param in list.named_children(&mut inner) {
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
        let qualname = build_qualname(ctx, &name);
        push_member(param, ctx, output, "property", name, qualname, None, None);
    }
}

/// `delegate void Handler(int x);`: a type-level declaration.
fn handle_delegate(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
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
    let signature = special_member_signature(node, source);
    push_member(
        node,
        ctx,
        output,
        "delegate",
        name,
        qualname,
        signature,
        Some(generic_arity(node)),
    );
}

/// Indexers (`this[]`), operators (`operator +`), conversion operators
/// (`implicit operator int`) and finalizers (`~Svc`). Overloads share one
/// qualname and differ by signature, like methods.
fn handle_special_member(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let (name, kind) = match node.kind() {
        "indexer_declaration" => ("this[]".to_string(), "property"),
        "destructor_declaration" => {
            let Some(name_node) = node.child_by_field_name("name") else {
                return;
            };
            (format!("~{}", node_text(name_node, source)), "method")
        }
        "conversion_operator_declaration" => {
            let Some(ty) = node.child_by_field_name("type") else {
                return;
            };
            let mut cursor = node.walk();
            let direction = node
                .children(&mut cursor)
                .find(|c| matches!(c.kind(), "implicit" | "explicit"))
                .map_or("implicit", |c| c.kind());
            let ty: Vec<String> = node_text(ty, source)
                .split_whitespace()
                .map(str::to_string)
                .collect();
            // No '.' in a name: parent lookups split the qualname on it.
            let ty = ty.join(" ").replace('.', "_");
            (format!("{direction} operator {ty}"), "method")
        }
        _ => {
            let Some(op) = node.child_by_field_name("operator") else {
                return;
            };
            (format!("operator {}", node_text(op, source)), "method")
        }
    };
    let qualname = build_qualname(ctx, &name);
    let signature = special_member_signature(node, source);
    let first_edge = output.edges.len();
    let (start_line, .., start_byte, _) = span(node);
    if has_modifier(node, source, "override") {
        output.override_symbols.push((qualname.clone(), start_line));
    }
    push_member(
        node,
        ctx,
        output,
        kind,
        name,
        qualname.clone(),
        signature,
        None,
    );
    walk_parameter_defaults(node, &qualname, ctx, source, output);
    let mut next_ctx = ctx.clone();
    next_ctx.fn_depth += 1;
    next_ctx.current_scope = qualname.clone();
    let (local_types, assigns) = infer_local_types(node, source, ctx);
    next_ctx.local_types = Rc::new(local_types);
    next_ctx.assigns = Rc::new(assigns);
    for field in ["body", "value"] {
        if let Some(body) = node.child_by_field_name(field) {
            walk_node(body, &next_ctx, source, output);
        }
    }
    walk_accessors(node, &qualname, ctx, source, output);
    pin_edge_sources(&mut output.edges[first_edge..], &qualname, start_byte);
}

/// `(params) -> type`, the shape `method_signature` gives methods; these
/// nodes name the type `type` rather than `returns`.
fn special_member_signature(node: Node<'_>, source: &str) -> Option<String> {
    let params = node_text(node.child_by_field_name("parameters")?, source);
    if params.is_empty() {
        return None;
    }
    match node
        .child_by_field_name("type")
        .map(|n| node_text(n, source))
    {
        Some(ty) if !ty.is_empty() => Some(format!("{params} -> {ty}")),
        _ => Some(params),
    }
}

/// Emit one member symbol spanning `node` plus the `CONTAINS` edge from its
/// declaring type or namespace.
#[allow(clippy::too_many_arguments)]
fn push_member(
    node: Node<'_>,
    ctx: &Context,
    output: &mut ExtractedFile,
    kind: &str,
    name: String,
    qualname: String,
    signature: Option<String>,
    own_arity: Option<usize>,
) {
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
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
        signature,
        docstring: None,
        identity: identity(ctx, own_arity),
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

fn handle_property(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = match explicit_interface_name(node, source) {
        Some(iface) => build_qualname(ctx, &format!("{iface}.{name}")),
        None => build_qualname(ctx, &name),
    };
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    if has_modifier(node, source, "override") {
        output.override_symbols.push((qualname.clone(), start_line));
    }
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
        identity: identity(ctx, None),
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(container_qualname(ctx)),
        target_qualname: Some(qualname.clone()),
        detail: None,
        evidence_snippet: None,
        ..Default::default()
    });
    walk_initializer(node, &qualname, ctx, source, output);
    walk_accessors(node, &qualname, ctx, source, output);
}

/// `event T Name { add {} remove {} }` / `event T IA.Name { ... }`.
fn handle_event(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let Some(name_node) = node.child_by_field_name("name") else {
        return;
    };
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = match explicit_interface_name(node, source) {
        Some(iface) => build_qualname(ctx, &format!("{iface}.{name}")),
        None => build_qualname(ctx, &name),
    };
    let is_override = has_modifier(node, source, "override");
    push_event(node, ctx, output, name, qualname.clone(), is_override);
    walk_accessors(node, &qualname, ctx, source, output);
}

/// Field-like `event EventHandler Changed, Other;`.
fn handle_event_field(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for decl in node.named_children(&mut cursor) {
        if decl.kind() != "variable_declaration" {
            continue;
        }
        let mut inner = decl.walk();
        for child in decl.named_children(&mut inner) {
            let Some(name_node) = (child.kind() == "variable_declarator")
                .then(|| child.child_by_field_name("name"))
                .flatten()
            else {
                continue;
            };
            let name = node_text(name_node, source);
            if name.is_empty() {
                continue;
            }
            let qualname = build_qualname(ctx, &name);
            let is_override = has_modifier(node, source, "override");
            push_event(child, ctx, output, name, qualname, is_override);
        }
    }
}

fn push_event(
    node: Node<'_>,
    ctx: &Context,
    output: &mut ExtractedFile,
    name: String,
    qualname: String,
    is_override: bool,
) {
    if is_override {
        output
            .override_symbols
            .push((qualname.clone(), span(node).0));
    }
    push_member(node, ctx, output, "event", name, qualname, None, None);
}

/// Walk a property/event's accessor bodies (`get`/`set`/`init`/`add`/
/// `remove`), attributing their calls to the member itself.
fn walk_accessors(
    node: Node<'_>,
    scope: &str,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let Some(list) = node.child_by_field_name("accessors") else {
        return;
    };
    let mut cursor = list.walk();
    for accessor in list.named_children(&mut cursor) {
        if accessor.kind() != "accessor_declaration" {
            continue;
        }
        let mut next_ctx = ctx.clone();
        next_ctx.fn_depth += 1;
        next_ctx.current_scope = scope.to_string();
        let (local_types, assigns) = infer_local_types(accessor, source, ctx);
        next_ctx.local_types = Rc::new(local_types);
        next_ctx.assigns = Rc::new(assigns);
        let mut inner = accessor.walk();
        for child in accessor.named_children(&mut inner) {
            walk_node(child, &next_ctx, source, output);
        }
    }
}

/// The `base(..)` / `this(..)` a constructor delegates to, as a CALLS edge
/// to the target type (the resolver picks the constructor by arity, as for
/// `new T(..)`). `None` when the base type isn't resolvable from this file.
fn constructor_initializer_edge(
    init: Node<'_>,
    ctor_qualname: &str,
    ctx: &Context,
    source: &str,
) -> Option<EdgeInput> {
    let text = node_text(init, source);
    let is_this = text
        .trim_start_matches(':')
        .trim_start()
        .starts_with("this");
    let target = if is_this {
        container_qualname(ctx)
    } else {
        // Qualified like `new Base()`'s target, so a same-named file module
        // (`Base.cs` -> module `Base`) never wins the exact tier.
        resolve_call_target(ctx.base_class_name.as_ref()?, ctx)?
    };
    if target.is_empty() {
        return None;
    }
    let (start_line, _, end_line, _, start_byte, end_byte) = span(init);
    // A `base` type is named the way it is in source: qualify it through this
    // file's `using`s and namespace so a same-named type elsewhere refuses
    // rather than wins.
    let import_candidates = match (is_this, ctx.base_class_name.as_deref()) {
        (false, Some(base)) if !base.contains('.') => import_qualified_candidates(base, "", ctx)
            .into_iter()
            .map(|c| c.trim_end_matches('.').to_string())
            .collect(),
        _ => Vec::new(),
    };
    Some(EdgeInput {
        kind: "CALLS".to_string(),
        source_qualname: Some(ctor_qualname.to_string()),
        target_qualname: Some(target),
        import_candidates,
        evidence_snippet: util::edge_evidence_snippet(
            source, start_byte, end_byte, start_line, end_line,
        ),
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        call_shape: Some(call_shape(init)),
        ..Default::default()
    })
}

/// Walk every expression of a field/property initializer (`= new T()`,
/// `= Make(..)`, `=> expr`, lambdas), attributing its calls to the member
/// itself. `node` is the property declaration or a field's declarator; the
/// initializer is every named child other than the name/type/accessors.
fn walk_initializer(
    node: Node<'_>,
    scope: &str,
    ctx: &Context,
    source: &str,
    output: &mut ExtractedFile,
) {
    let mut next_ctx = ctx.clone();
    next_ctx.current_scope = scope.to_string();
    let skipped: Vec<_> = ["name", "type", "accessors"]
        .iter()
        .filter_map(|f| node.child_by_field_name(f))
        .collect();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if skipped.contains(&child)
            || matches!(
                child.kind(),
                "modifier" | "attribute_list" | "accessor_list"
            )
        {
            continue;
        }
        walk_node(child, &next_ctx, source, output);
    }
}

fn handle_field(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    // `const` is implicitly static. Recorded so `dead_symbols` can tell a
    // constants holder from a type with only instance state (#238): a
    // static-field read leaves no edge.
    let non_private = ["public", "internal", "protected"]
        .iter()
        .any(|m| has_modifier(node, source, m));
    let is_static = has_modifier(node, source, "const")
        || (has_modifier(node, source, "static") && non_private);
    let first_symbol = output.symbols.len();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "variable_declaration" {
            continue;
        }
        handle_variable_declaration(child, ctx, source, output);
    }
    if is_static {
        let fields: Vec<String> = output.symbols[first_symbol..]
            .iter()
            .filter(|s| s.kind == "field")
            .map(|s| s.qualname.clone())
            .collect();
        output.static_member_qualnames.extend(fields);
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
            identity: identity(ctx, None),
        });
        output.edges.push(EdgeInput {
            kind: "CONTAINS".to_string(),
            source_qualname: Some(container_qualname(ctx)),
            target_qualname: Some(qualname.clone()),
            detail: None,
            evidence_snippet: None,
            ..Default::default()
        });
        walk_initializer(child, &qualname, ctx, source, output);
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
    // An alias's target text is the alias itself (`using Alias = Z;` in
    // `Alias.cs` would import its own file module); point the edge at what
    // the alias names and keep the alias for the resolver (see `using_context`).
    let mut aliased = ImportContext::default();
    record_using_directive(node, source, &mut aliased);
    let (target, detail) = match aliased.aliases.into_iter().next() {
        Some((alias, named)) => (
            named.clone(),
            Some(json!({ "alias": alias, "target": named }).to_string()),
        ),
        None => (target, None),
    };
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    output.edges.push(EdgeInput {
        kind: "IMPORTS".to_string(),
        source_qualname: Some(base_qualname(ctx)),
        target_qualname: Some(target),
        detail,
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
    // Target-typed `new(..)` has no type node: the declared type it is
    // assigned to (issue #186) names the constructed type instead.
    let implicit_type = (node.kind() == "implicit_object_creation_expression")
        .then(|| target_typed_new_type(node, ctx, source));
    let target_node = call_target_node(node);
    if matches!(implicit_type, Some(None))
        && let Some((marker, callee)) = deferred_argument_marker(node, ctx, source)
    {
        let (start_line, _, end_line, _, start_byte, end_byte) = span(node);
        output.edges.push(EdgeInput {
            kind: "CALLS".to_string(),
            source_qualname: Some(ctx.current_scope.clone()),
            // The callee's name keeps the edge retryable; the resolver binds
            // the constructor of the callee's parameter type instead.
            target_qualname: Some(callee),
            evidence_snippet: util::edge_evidence_snippet(
                source, start_byte, end_byte, start_line, end_line,
            ),
            receiver_type: marker,
            evidence_start_line: Some(start_line),
            evidence_end_line: Some(end_line),
            call_shape: Some(call_shape(node)),
            ..Default::default()
        });
        return;
    }
    if target_node.is_none() && !matches!(implicit_type, Some(Some(_))) {
        return;
    }
    // ponytail: `new List<T>()` keeps its type args (and so stays
    // unresolved): stripped to `List`, a BCL generic type falls to the
    // bare-name tier and binds a same-named repo *method* (51 edges to a
    // gRPC `List` rpc on dpb, for 3 genuine repo generic classes gained).
    // Upgrade path: a constructor-only kind filter (class/struct/record)
    // in the fuzzy tiers, then strip here too.
    let raw = match (implicit_type.flatten(), target_node) {
        (Some(declared), _) => declared,
        (None, Some(target)) if node.kind() == "object_creation_expression" => {
            node_text(target, source)
        }
        (None, Some(target)) => call_target_text(target, source),
        (None, None) => return,
    };
    if raw.is_empty() {
        return;
    }
    // An unqualified call inside a type body has an implicit receiver (see
    // `CallShape::implicit_this`). A local function is no symbol, so a call
    // to one binds to nothing: it must not fall on to a same-named member.
    let implicit_this = node.kind() == "invocation_expression"
        && !ctx.type_stack.is_empty()
        && target_node.is_some_and(|target| match target.kind() {
            "identifier" => true,
            // `this.Foo()` names the same receiver explicitly.
            "member_access_expression" => {
                target
                    .child_by_field_name("expression")
                    .is_some_and(|e| e.kind() == "this")
                    && target
                        .child_by_field_name("name")
                        .is_some_and(|n| n.kind() == "identifier")
            }
            _ => false,
        });
    let bare_identifier = target_node.is_some_and(|target| target.kind() == "identifier");
    if bare_identifier && implicit_this && calls_local_function(node, &raw, source) {
        return;
    }
    // Invoking a delegate held in a local or parameter is no method call.
    if bare_identifier && ctx.local_types.contains_key(raw.as_str()) {
        return;
    }
    let receiver_type = target_node.map_or(ReceiverType::NotTracked, |target| {
        infer_receiver_type(target, source, ctx)
    });
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
    let extension_candidates = target_node
        .and_then(|target| call_target_parts(target, source))
        .filter(|parts| parts.receiver.is_some())
        .map(|parts| {
            // `call_target_parts` keeps `<T>` for the CONFIG_BIND/HTTP
            // detectors; an extension method's symbol name never has it.
            let name = parts.name.split('<').next().unwrap_or(&parts.name);
            extension_method_candidates(name, &receiver_type, ctx)
        })
        .unwrap_or_default();
    // Union rather than replace: on the rare chance both tiers produce a
    // (necessarily different) candidate, let `Resolver::resolve_import`'s
    // own ambiguity guard see both and refuse rather than silently
    // preferring one.
    let mut import_candidates = type_call_candidates;
    for candidate in extension_candidates {
        if !import_candidates.contains(&candidate) {
            import_candidates.push(candidate);
        }
    }
    let mut receiver_type = receiver_type;
    let mut target = resolve_call_target(&raw, ctx);
    if target.is_none()
        && let ReceiverType::Deferred(call) = &mut receiver_type
        && let Some(parts) = target_node.and_then(|target| call_target_parts(target, source))
    {
        // `a.B().C()`: no printable receiver, so the target is just `C`,
        // bound through the deferred receiver type alone.
        call.name_only = true;
        target = Some(
            parts
                .name
                .split('<')
                .next()
                .unwrap_or(&parts.name)
                .to_string(),
        );
    }
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
        // A bare identifier callee (`Foo()`) vs. anything qualified
        // (`this.Foo()`, `obj.Foo()`, ...) — see `EdgeInput::bare_call`'s
        // doc. Two exceptions where `Foo()`-shaped text still isn't
        // "bare" for gating purposes:
        // - `new Foo()`: a constructor call has no receiver concept at
        //   all, and its target is a `method`-kind (`.ctor`) symbol.
        // - Any unqualified call inside a class/struct/interface body
        //   (`ctx.type_stack` non-empty): C# gives it an implicit `this`
        //   (or, for a static caller, the enclosing type itself) —
        //   unlike a free function call in Python/Go/Rust/TS, it always
        //   has a receiver, just not a written one (issue #75 follow-up,
        //   finding C).
        bare_call: node.kind() == "invocation_expression"
            && target_node.is_some_and(|target| target.kind() == "identifier")
            && ctx.type_stack.is_empty(),
        call_shape: Some(CallShape {
            implicit_this,
            ..call_shape(node)
        }),
        ..Default::default()
    });
}

/// Whether `name` is a local function declared in a block enclosing `node`
/// (within its own method, including from inside a lambda body, whose blocks
/// are ancestors like any other): C# binds an unqualified call to it first.
/// A local function is not an indexed symbol, so the caller drops the edge
/// altogether rather than let it bind to a same-named member (deliberate:
/// no edge is more honest than a wrong one).
fn calls_local_function(node: Node<'_>, name: &str, source: &str) -> bool {
    let mut current = node.parent();
    while let Some(ancestor) = current {
        // The enclosing member or type declaration bounds the search: a
        // local function is only visible inside the member that declares
        // it. (Not a `_declaration` suffix test: `variable_declaration`
        // sits inside method bodies.)
        if matches!(
            ancestor.kind(),
            "method_declaration"
                | "constructor_declaration"
                | "destructor_declaration"
                | "operator_declaration"
                | "conversion_operator_declaration"
                | "accessor_declaration"
                | "property_declaration"
                | "indexer_declaration"
                | "event_declaration"
                | "field_declaration"
                | "class_declaration"
                | "struct_declaration"
                | "record_declaration"
                | "interface_declaration"
        ) {
            return false;
        }
        let mut cursor = ancestor.walk();
        if ancestor.named_children(&mut cursor).any(|child| {
            is_local_function_node(child.kind())
                && child
                    .child_by_field_name("name")
                    .is_some_and(|n| node_text(n, source) == name)
        }) {
            return true;
        }
        current = ancestor.parent();
    }
    false
}

/// The argument count / object-creation marker of a call or `new` node.
fn call_shape(node: Node<'_>) -> CallShape {
    // A target-typed `new(..)` carries its `argument_list` unnamed.
    let arg_count = node
        .child_by_field_name("arguments")
        .or_else(|| {
            let mut cursor = node.walk();
            node.named_children(&mut cursor)
                .find(|c| c.kind() == "argument_list")
        })
        .map(|args| {
            let mut cursor = args.walk();
            args.named_children(&mut cursor)
                .filter(|c| c.kind() == "argument")
                .count() as u32
        })
        .unwrap_or(0);
    CallShape {
        arg_count,
        is_new: node.kind() != "invocation_expression",
        implicit_this: false,
    }
}

/// Deferred-argument marker for a target-typed `new(..)` passed as a call argument
/// (`Foo(new())`, `recv.M(1, x: new(2))`): the resolver finishes it from the
/// callee's declared parameter type (`ReceiverType::deferred_argument`).
/// `None` when the callee can't be named from this file alone.
fn deferred_argument_marker(
    node: Node<'_>,
    ctx: &Context,
    source: &str,
) -> Option<(ReceiverType, String)> {
    let arg = node.parent().filter(|p| p.kind() == "argument")?;
    let list = arg.parent().filter(|p| p.kind() == "argument_list")?;
    let call = list.parent()?;
    let mut cursor = list.walk();
    let args: Vec<Node<'_>> = list
        .named_children(&mut cursor)
        .filter(|c| c.kind() == "argument")
        .collect();
    let index = args.iter().position(|a| *a == arg)?;
    let name = arg
        .child_by_field_name("name")
        .map(|n| node_text(n, source));
    let container = container_qualname(ctx);
    let callee = match call.kind() {
        "invocation_expression" => {
            let function = call.child_by_field_name("function")?;
            let method = |n: Node<'_>| {
                let text = node_text(n, source);
                let bare = text.split('<').next().unwrap_or(&text).to_string();
                (!bare.is_empty()).then_some(bare)
            };
            match function.kind() {
                "identifier" | "generic_name" if !container.is_empty() => {
                    format!("{container}.{}", method(function)?)
                }
                "member_access_expression" => {
                    let method = method(function.child_by_field_name("name")?)?;
                    let receiver = function.child_by_field_name("expression")?;
                    if receiver.kind() == "this" {
                        if container.is_empty() {
                            return None;
                        }
                        format!("{container}.{method}")
                    } else {
                        match infer_receiver_type(function, source, ctx) {
                            ReceiverType::Known(ty) | ReceiverType::Scoped { ty, .. } => {
                                format!("{ty}.{method}")
                            }
                            // A bare type name (`Helper.Make(new())`).
                            ReceiverType::NotTracked
                                if receiver.kind() == "identifier"
                                    && node_text(receiver, source)
                                        .starts_with(|c: char| c.is_ascii_uppercase()) =>
                            {
                                format!("{}.{method}", node_text(receiver, source))
                            }
                            _ => return None,
                        }
                    }
                }
                _ => return None,
            }
        }
        "constructor_initializer" => {
            let text = node_text(call, source);
            if text
                .trim_start_matches(':')
                .trim_start()
                .starts_with("this")
            {
                format!("{container}..ctor")
            } else {
                format!("{}..ctor", ctx.base_class_name.as_ref()?)
            }
        }
        "object_creation_expression" => {
            let ty = node_text(call.child_by_field_name("type")?, source);
            if ty.is_empty() || !is_simple_call_target(&ty) {
                return None;
            }
            format!("{ty}..ctor")
        }
        _ => return None,
    };
    let marker = ReceiverType::DeferredArgument(DeferredArgument {
        index,
        name,
        arg_count: args.len(),
        callee: callee.clone(),
    });
    Some((marker, callee))
}

/// The constructed type of a target-typed `new(..)`: the declared type of the
/// field/local/property/parameter it initialises or is assigned to, or of the
/// method/accessor it is returned from. `None` for `var`, builtin or generic
/// types (`classify_annotation` yields no `Known` name) and every context
/// where no declared type is in reach.
fn target_typed_new_type(node: Node<'_>, ctx: &Context, source: &str) -> Option<String> {
    let mut child = node;
    let mut parent = node.parent()?;
    // Wrappers that pass the target type through to the `new(..)` inside:
    // both `?:` branches and the right of `??`.
    loop {
        let passes_through = match parent.kind() {
            "parenthesized_expression" | "equals_value_clause" => true,
            "conditional_expression" => parent.child_by_field_name("condition") != Some(child),
            "binary_expression" => {
                parent.child_by_field_name("right") == Some(child)
                    && parent
                        .child_by_field_name("operator")
                        .is_some_and(|op| node_text(op, source) == "??")
            }
            _ => false,
        };
        if !passes_through {
            break;
        }
        child = parent;
        parent = parent.parent()?;
    }
    let declared = match parent.kind() {
        "variable_declarator" => parent
            .parent()
            .filter(|decl| decl.kind() == "variable_declaration")?
            .child_by_field_name("type")
            .map(|t| node_text(t, source)),
        "property_declaration" | "parameter" => parent
            .child_by_field_name("type")
            .map(|t| node_text(t, source)),
        "assignment_expression" => {
            let left = parent.child_by_field_name("left")?;
            if parent.child_by_field_name("right") != Some(child) {
                return None;
            }
            let name = match left.kind() {
                "identifier" => node_text(left, source),
                "member_access_expression"
                    if left.child_by_field_name("expression").map(|e| e.kind()) == Some("this") =>
                {
                    node_text(left.child_by_field_name("name")?, source)
                }
                _ => return None,
            };
            let local = if left.kind() == "identifier" {
                ctx.local_types.get(&name)
            } else {
                None
            };
            return match local.or_else(|| ctx.class_attr_types.get(&name)) {
                Some(LocalType::Known(ty)) => Some(ty.clone()),
                _ => None,
            };
        }
        "return_statement" | "arrow_expression_clause" => enclosing_return_type(parent, source),
        k if is_lambda_node(k) => lambda_return_type(parent, source),
        _ => None,
    }?;
    match classify_annotation(&declared) {
        LocalType::Known(_) => Some(declared.trim().trim_end_matches('?').trim().to_string()),
        _ => None,
    }
}

/// Declared return type of the method, local function, property accessor or
/// lambda a `return` statement / expression body belongs to, with an async
/// `Task<T>` / `ValueTask<T>` unwrapped to `T` (and a non-async one refused:
/// there `new()` would construct the task itself).
fn enclosing_return_type(from: Node<'_>, source: &str) -> Option<String> {
    let mut current = from.parent();
    while let Some(node) = current {
        match node.kind() {
            "method_declaration" | "local_function_statement" => {
                let declared = node
                    .child_by_field_name("returns")
                    .or_else(|| node.child_by_field_name("type"))
                    .map(|t| node_text(t, source))?;
                return unwrap_return(&declared, has_modifier(node, source, "async"));
            }
            "property_declaration" | "indexer_declaration" => {
                return node
                    .child_by_field_name("type")
                    .map(|t| node_text(t, source));
            }
            k if is_lambda_node(k) => return lambda_return_type(node, source),
            _ => current = node.parent(),
        }
    }
    None
}

/// The return type a lambda's declared delegate type gives it: the last type
/// argument of `Func<..>` on the declaration the lambda initialises, with
/// async unwrapping as for a method. Any other delegate type is unknown.
fn lambda_return_type(lambda: Node<'_>, source: &str) -> Option<String> {
    let mut parent = lambda.parent()?;
    if parent.kind() == "equals_value_clause" {
        parent = parent.parent()?;
    }
    if parent.kind() != "variable_declarator" {
        return None;
    }
    let delegate = node_text(
        parent
            .parent()
            .filter(|decl| decl.kind() == "variable_declaration")?
            .child_by_field_name("type")?,
        source,
    );
    let inner = delegate
        .trim()
        .strip_prefix("Func<")
        .and_then(|rest| rest.strip_suffix('>'))?;
    let ret = split_respecting_brackets(inner).pop()?;
    let is_async = node_text(lambda, source).trim_start().starts_with("async");
    unwrap_return(ret.trim(), is_async)
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
    method_returns: &MethodReturns,
) -> HashMap<String, (String, Option<String>)> {
    let mut clients = HashMap::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "global_statement" {
            continue;
        }
        collect_grpc_clients_inner(child, source, method_returns, &mut clients);
    }
    clients
}

fn collect_grpc_clients(
    node: Node<'_>,
    source: &str,
    method_returns: &MethodReturns,
) -> HashMap<String, (String, Option<String>)> {
    let mut clients = HashMap::new();
    collect_grpc_clients_inner(node, source, method_returns, &mut clients);
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
    method_returns: &MethodReturns,
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
            collect_grpc_clients_from_declaration(node, source, method_returns, clients);
            return;
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_grpc_clients_inner(child, source, method_returns, clients);
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
    method_returns: &MethodReturns,
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
        // `var c = CreateClient(..)`: fall back to the same-file callee's
        // declared return type (`Greeter.GreeterClient CreateClient(..)`).
        let from_return = || {
            let value = variable_declarator_value(declarator)?;
            let ret = method_returns.call_return_type(value, source)?;
            split_client_service_and_prefix(&ret)
        };
        if let Some(service_and_prefix) = from_initializer
            .or_else(|| declared.clone())
            .or_else(from_return)
        {
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
///
/// Only `public override` methods qualify (#125): a generated
/// `*ServiceBase` class's actual RPC methods are always `public override`,
/// so a `private`/`private static` helper living in the same class —
/// however plausible its name — must never get an RPC_IMPL edge.
fn grpc_impl_edge(node: Node<'_>, ctx: &Context, source: &str, rpc_name: &str) -> Vec<EdgeInput> {
    let Some(service) = ctx.grpc_service.as_deref() else {
        return Vec::new();
    };
    if !has_modifier(node, source, "public") || !has_modifier(node, source, "override") {
        return Vec::new();
    }
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    let source_qualname = build_qualname(ctx, rpc_name);
    // ponytail: when no candidate package could be derived at all (no
    // base-list prefix and no bare `using` in the file), fall back to a
    // single package-less candidate rather than emitting nothing — covers
    // a proto file with no `package` statement, and top-level impl classes.
    // Upgrade path: none needed unless a real cross-file symbol table (like
    // `db::resolver::Resolver::resolve_import`'s) becomes available to this
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

/// Sentinel `EdgeInput::kind` for a not-yet-resolved gRPC client call —
/// never a real edge kind, never reaches the DB. `grpc_call_edge` emits
/// this instead of an `RPC_CALL` edge when a call site's receiver doesn't
/// resolve to a gRPC client *locally* (same file): the receiver might
/// still be a gRPC-client-typed field/property declared in a *different*
/// file (dpb's actual shape — see `GrpcClientFieldRegistry`'s doc), which
/// can't be known for certain until every file's fields have been seen,
/// regardless of which file happens to get `extract()`-ed first.
/// `resolve_pending_grpc_calls` (called from `resolve_imports`, once the
/// whole-repo prescan is guaranteed complete) turns every one of these
/// into zero or more real `RPC_CALL` edges and removes the placeholder —
/// `extract_file` in `indexer/mod.rs` always calls `resolve_imports` right
/// after `extract()` for the same file, so no placeholder can survive past
/// that pairing.
const PENDING_GRPC_CLIENT_CALL_KIND: &str = "__pending_grpc_client_call__";

/// Builds an `RPC_CALL` edge (or a `PENDING_GRPC_CLIENT_CALL_KIND`
/// placeholder for later — see that constant's doc) mirroring
/// `grpc_impl_edge`'s treatment of `RPC_IMPL` (see that function's doc) —
/// the client side had the identical CLR-namespace bug `1c83726` fixed for
/// the impl side: the generated `{Service}.{Service}Client` type's own
/// qualifying prefix (from `new {prefix.}{Service}.{Service}Client(...)`,
/// captured by `grpc_client_from_object_creation` and carried in
/// `ctx.grpc_clients`), not `ctx.namespace_stack` (the *calling* code's own
/// CLR namespace, which has no reliable relationship to the proto package a
/// client it happens to construct belongs to), is what determines the
/// package.
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
    let (start_line, _start_col, end_line, _end_col, start_byte, end_byte) = span(node);
    let snippet = util::edge_evidence_snippet(source, start_byte, end_byte, start_line, end_line);
    let source_qualname = ctx.current_scope.clone();

    // Local (this-file) resolution first — a receiver that's itself an
    // inline construction, or a locally-bound variable/field/property this
    // *same* file's `ctx.grpc_clients` already knows about (see
    // `handle_type`'s merge for the field/property case). Both are
    // order-independent (nothing outside this file is consulted), so
    // resolving them here, immediately, is safe.
    if let Some(service_and_prefix) = grpc_service_from_client_receiver(target.receiver.as_deref())
        .or_else(|| grpc_service_from_client_binding(target.receiver.as_deref(), ctx))
    {
        return build_grpc_call_edges(
            &[service_and_prefix],
            &rpc_name,
            &source_qualname,
            &snippet,
            start_line,
            end_line,
            &ctx.imports,
        );
    }

    // Local resolution found nothing. Rather than guess using a registry
    // that's only reliable once every file has been seen (see
    // `PENDING_GRPC_CLIENT_CALL_KIND`'s doc), defer. Cheap prefilter: only
    // bother when the receiver's trailing segment itself looks like a
    // generated client accessor (ends in "Client", the same suffix every
    // real gRPC client field in dpb's corpus uses, e.g. `scope.Client`) —
    // this is *not* the corroboration gate (that's still entirely
    // `split_client_service_and_prefix`'s mandatory self-reference check,
    // applied in `resolve_pending_grpc_calls` via the now-complete
    // registry), just a volume control so a placeholder isn't allocated
    // for every unrelated method call in the file (`logger.LogInformation(...)`,
    // `list.Add(...)`, ...). A real client field named something that
    // doesn't end in "Client" would still be missed here — same trade-off
    // `grpc_client_field_candidate`'s doc explains.
    let Some(field_name) = grpc_client_field_candidate(target.receiver.as_deref()) else {
        return Vec::new();
    };
    vec![pending_grpc_client_call_edge(
        &field_name,
        &rpc_name,
        &source_qualname,
        &snippet,
        start_line,
        end_line,
        ctx,
    )]
}

/// Builds an `RPC_CALL` edge per candidate `(service, protobuf package)`
/// pair — one `(service, prefix)` when resolved locally
/// (`grpc_call_edge`'s own immediate path), or several when resolved from
/// the cross-file registry (`resolve_pending_grpc_calls`, where the
/// receiver's own declaring type is invisible so every candidate the
/// registry has under that field name is tried): crossed with candidate
/// packages, a wrong `(service, package)` pair just never matches a real
/// `RPC_IMPL`/`RPC_ROUTE` target downstream, so fanning out rather than
/// picking a winner is safe either way. Reuses
/// `grpc_package_candidates_from_prefix` rather than duplicating its
/// alias-resolution/bare-`using`-fallback logic.
fn build_grpc_call_edges(
    services: &[(String, Option<String>)],
    rpc_name: &str,
    source_qualname: &str,
    snippet: &Option<String>,
    start_line: i64,
    end_line: i64,
    imports: &ImportContext,
) -> Vec<EdgeInput> {
    let mut seen_targets = std::collections::HashSet::new();
    let mut edges = Vec::new();
    for (service, prefix) in services {
        // Same ponytail fallback as `grpc_impl_edge`: no derivable prefix
        // and no bare `using` in the file still emits one package-less
        // candidate rather than nothing, covering a proto file with no
        // `package` statement.
        let packages: Vec<Option<String>> = match grpc_package_candidates_from_prefix(
            prefix.as_deref(),
            &imports.aliases,
            &imports.namespaces,
        ) {
            candidates if candidates.is_empty() => vec![None],
            candidates => candidates.into_iter().map(Some).collect(),
        };
        for package in packages {
            let Some((raw_path, normalized)) =
                proto::normalize_rpc_path(package.as_deref(), service, rpc_name)
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
                source_qualname: Some(source_qualname.to_string()),
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

/// The trailing dotted segment of a receiver, when it looks like a
/// generated-client accessor (ends in `"Client"`) — see
/// `grpc_call_edge`'s doc for why this prefilter exists and what it
/// trades away. `None` when there's no receiver at all (a bare, unqualified
/// call — nothing to defer) or its trailing segment doesn't end in
/// `"Client"`.
fn grpc_client_field_candidate(receiver: Option<&str>) -> Option<String> {
    let receiver = receiver.map(str::trim).filter(|r| !r.is_empty())?;
    let last = receiver.rsplit('.').next().unwrap_or(receiver);
    if last.is_empty() || !last.ends_with("Client") {
        return None;
    }
    Some(last.to_string())
}

/// Builds a `PENDING_GRPC_CLIENT_CALL_KIND` placeholder carrying everything
/// `resolve_pending_grpc_calls` needs to finish resolving this call site
/// once the whole-repo field registry is complete: the candidate field
/// name to look up, the rpc name, and this *calling* file's own import
/// context (`ctx.imports`, captured as plain data since the `Context`/AST
/// this came from won't exist any more by the time `resolve_imports` runs
/// for this file).
fn pending_grpc_client_call_edge(
    field_name: &str,
    rpc_name: &str,
    source_qualname: &str,
    snippet: &Option<String>,
    start_line: i64,
    end_line: i64,
    ctx: &Context,
) -> EdgeInput {
    let detail = json!({
        "pending_field": field_name,
        "pending_rpc": rpc_name,
        "pending_namespaces": ctx.imports.namespaces,
        "pending_aliases": ctx.imports.aliases,
    })
    .to_string();
    EdgeInput {
        kind: PENDING_GRPC_CLIENT_CALL_KIND.to_string(),
        source_qualname: Some(source_qualname.to_string()),
        target_qualname: None,
        detail: Some(detail),
        evidence_snippet: snippet.clone(),
        evidence_start_line: Some(start_line),
        evidence_end_line: Some(end_line),
        ..Default::default()
    }
}

/// Turns every `PENDING_GRPC_CLIENT_CALL_KIND` placeholder in `edges` into
/// zero or more real `RPC_CALL` edges (via `build_grpc_call_edges`) using
/// `registry`, then removes the placeholders — called from
/// `resolve_imports` once `registry` (a whole-repo field-declaration
/// prescan) is guaranteed complete, so unlike the placeholder's own
/// creation in `grpc_call_edge`, this never depends on file processing
/// order. A field name with no registry entry (the overwhelming majority —
/// see `grpc_client_field_candidate`'s prefilter, which lets plenty of
/// non-client `...Client`-suffixed placeholders through, e.g. a local
/// `blobService.GetBlobContainerClient(...)`-style receiver — corroboration
/// happens here, not there) simply produces no edge for that placeholder.
fn resolve_pending_grpc_calls(edges: &mut Vec<EdgeInput>, registry: &GrpcClientFieldRegistry) {
    let mut pending = Vec::new();
    edges.retain(|edge| {
        if edge.kind == PENDING_GRPC_CLIENT_CALL_KIND {
            pending.push(edge.clone());
            false
        } else {
            true
        }
    });
    if pending.is_empty() {
        return;
    }
    let registry = registry.borrow();
    for edge in pending {
        let Some(detail) = edge.detail.as_deref() else {
            continue;
        };
        let Ok(payload) = serde_json::from_str::<serde_json::Value>(detail) else {
            continue;
        };
        let Some(field_name) = payload.get("pending_field").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(rpc_name) = payload.get("pending_rpc").and_then(|v| v.as_str()) else {
            continue;
        };
        let Some(candidates) = registry.get(field_name) else {
            continue;
        };
        let namespaces: Vec<String> = payload
            .get("pending_namespaces")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let aliases: HashMap<String, String> = payload
            .get("pending_aliases")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default();
        let imports = ImportContext {
            namespaces,
            aliases,
        };
        let source_qualname = edge.source_qualname.clone().unwrap_or_default();
        edges.extend(build_grpc_call_edges(
            candidates,
            rpc_name,
            &source_qualname,
            &edge.evidence_snippet,
            edge.evidence_start_line.unwrap_or_default(),
            edge.evidence_end_line.unwrap_or_default(),
            &imports,
        ));
    }
}

/// One-time, whole-repo scan for every gRPC-client-typed field/property
/// declaration in every `.cs` file under `repo_root` — independent of
/// `extract()`'s own per-file, streaming processing order. See
/// `GrpcClientFieldRegistry`'s doc for why this needs to happen up front:
/// dpb's real call sites (`scope.Client.Method()`) are routinely processed
/// *before* the file that declares `Client`, and an incrementally-built
/// registry has nothing to offer at that point no matter what order the
/// repo happens to sort into. Called from `resolve_imports`, guarded by
/// `CSharpExtractor::grpc_prescan_done` so it only ever runs once per
/// reindex.
///
/// Reuses `scan::scan_repo` (the same file discovery `Indexer` itself
/// uses) for gitignore-aware traversal rather than reimplementing it, at
/// the cost of walking the repo a second time — this only ever happens
/// once per reindex, not once per file. Deliberately lightweight beyond
/// that: parses every C# file with a throwaway `Parser` and walks only for
/// `class_declaration`/`struct_declaration`/`record_declaration` bodies,
/// feeding each straight into `collect_class_level_grpc_client_fields` —
/// none of `extract()`'s other work (symbols, CALLS, HTTP/config/channel
/// detection, ...) runs here. An unreadable file, a parse failure, or a
/// failed repo scan leaves `registry` however much it already collected —
/// not fatal, matching `extract()`'s own per-file failure handling.
fn prescan_grpc_client_fields(repo_root: &Path, registry: &GrpcClientFieldRegistry) {
    let Ok(files) = scan::scan_repo(repo_root) else {
        return;
    };
    let mut parser = Parser::new();
    if parser
        .set_language(&tree_sitter_c_sharp::LANGUAGE.into())
        .is_err()
    {
        return;
    }
    for file in files {
        if file.language != "csharp" {
            continue;
        }
        let Ok(source) = std::fs::read_to_string(&file.abs_path) else {
            continue;
        };
        let Some(tree) = parser.parse(&source, None) else {
            continue;
        };
        let method_returns = MethodReturns::collect(tree.root_node(), &source);
        collect_grpc_client_fields_from_tree(tree.root_node(), &source, &method_returns, registry);
    }
}

fn collect_grpc_client_fields_from_tree(
    node: Node<'_>,
    source: &str,
    method_returns: &MethodReturns,
    registry: &GrpcClientFieldRegistry,
) {
    if matches!(
        node.kind(),
        "class_declaration" | "struct_declaration" | "record_declaration"
    ) && let Some(body) = node.child_by_field_name("body")
    {
        let fields = collect_class_level_grpc_client_fields(body, source, method_returns);
        if !fields.is_empty() {
            let mut reg = registry.borrow_mut();
            for (name, service_and_prefix) in fields {
                let entries = reg.entry(name).or_default();
                if !entries.contains(&service_and_prefix) {
                    entries.push(service_and_prefix);
                }
            }
        }
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_grpc_client_fields_from_tree(child, source, method_returns, registry);
    }
}

/// What the enclosing member says about a bare-identifier argument: a
/// parameter or a name declared/assigned more than once is not static; a
/// single declaration yields its initializer; no binding falls back to the
/// file's constants.
fn csharp_local_binding(call: Node<'_>, name: &str, source: &str) -> LocalBinding {
    let name = name.trim();
    if name.contains('.') || name.is_empty() {
        return LocalBinding::NotLocal;
    }
    let kinds = [
        "method_declaration",
        "constructor_declaration",
        "local_function_statement",
        "accessor_declaration",
        "operator_declaration",
        "destructor_declaration",
        "conversion_operator_declaration",
    ];
    scan_enclosing_function(call, &kinds, |n, tally| match n.kind() {
        "parameter" => {
            if n.child_by_field_name("name")
                .is_some_and(|x| node_text(x, source) == name)
            {
                tally.other_bindings += 1;
            }
        }
        "variable_declarator" => {
            if n.child_by_field_name("name")
                .is_some_and(|x| node_text(x, source) == name)
            {
                tally.declarations += 1;
                tally.initializer = csharp_declarator_initializer(n).map(|i| node_text(i, source));
            }
        }
        "assignment_expression" => {
            if n.child_by_field_name("left")
                .is_some_and(|x| node_text(x, source) == name)
            {
                tally.reassignments += 1;
            }
        }
        "declaration_pattern" | "for_each_statement" | "catch_declaration" => {
            let binds = node_text(n, source)
                .split(|c: char| !c.is_alphanumeric() && c != '_')
                .any(|t| t == name);
            if binds {
                tally.other_bindings += 1;
            }
        }
        _ => {}
    })
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
    let local = csharp_local_binding(node, &raw_topic, source);
    let normalized = channel::resolve_topic(&raw_topic, &ctx.string_consts, &local)?;
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
    let local = csharp_local_binding(node, &raw_topic, source);
    let normalized = channel::resolve_topic(&raw_topic, &ctx.string_consts, &local)?;
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
///
/// Takes `aliases`/`namespaces` directly (a calling file's own
/// `ImportContext`, unpacked) rather than a `&Context`, so
/// `resolve_pending_grpc_calls` can call this with values deserialized out
/// of a `PENDING_GRPC_CLIENT_CALL_KIND` placeholder's `detail` — by the
/// time that runs, the original `Context`/AST for the calling file is long
/// gone; only the plain data captured in the placeholder survives.
fn grpc_package_candidates_from_prefix(
    prefix: Option<&str>,
    aliases: &HashMap<String, String>,
    namespaces: &[String],
) -> Vec<String> {
    if let Some(prefix) = prefix {
        if let Some(fqn) = aliases.get(prefix) {
            return vec![fqn.clone()];
        }
        return vec![prefix.to_string()];
    }
    let mut seen = std::collections::HashSet::new();
    let mut candidates = Vec::new();
    for ns in namespaces {
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

/// Resolves a call-site receiver to a `(service, prefix)` using only
/// *this file's* own `ctx.grpc_clients` — an exact match (locally-bound
/// variable, or a field/property of the directly enclosing type), then the
/// receiver's trailing segment against the same map (`obj.client.Method()`
/// -style single-hop field access within this file). Both are
/// order-independent (nothing outside this file is consulted), unlike the
/// cross-file case: see `grpc_client_field_candidate` /
/// `PENDING_GRPC_CLIENT_CALL_KIND` for how a receiver naming a
/// different file's field/property is handled instead.
fn grpc_service_from_client_binding(
    receiver: Option<&str>,
    ctx: &Context,
) -> Option<(String, Option<String>)> {
    let receiver = receiver.map(str::trim).filter(|r| !r.is_empty())?;
    if let Some(service_and_prefix) = ctx.grpc_clients.get(receiver) {
        return Some(service_and_prefix.clone());
    }
    let last = receiver.rsplit('.').next().unwrap_or(receiver);
    ctx.grpc_clients.get(last).cloned()
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

/// A callee's text with the method's explicit generic type-argument list
/// dropped: `_sql.QueryAsync<long?>` -> `_sql.QueryAsync`, `Helper<int>` ->
/// `Helper`. tree-sitter-c-sharp parses that list as part of a
/// `generic_name` (the whole callee, or a `member_access_expression`'s
/// `name`); left in, `is_simple_call_target` rejects the `<`/`>` and the
/// CALLS edge's `target_qualname` ends up empty. Type arguments on the
/// *receiver* (`Foo<int>.Bar()`) are left alone.
fn call_target_text(node: Node<'_>, source: &str) -> String {
    let generic = match node.kind() {
        "generic_name" => Some(node),
        "member_access_expression" => node
            .child_by_field_name("name")
            .filter(|name| name.kind() == "generic_name"),
        _ => None,
    };
    let type_args = generic.and_then(|g| {
        let mut cursor = g.walk();
        g.named_children(&mut cursor)
            .find(|child| child.kind() == "type_argument_list")
    });
    match type_args {
        Some(args) => source
            .get(node.start_byte()..args.start_byte())
            .unwrap_or("")
            .trim()
            .to_string(),
        None => node_text(node, source),
    }
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

/// A C# local function (`void Helper() { ... }` declared inside a method
/// body). Unlike a lambda, this is a genuinely separate named scope — it
/// could reasonably become its own symbol one day — so `walk_node` and
/// `collect_statement_bindings` both still treat it as a hard boundary and
/// its calls remain unindexed. Narrower than fixing `is_lambda_node` below,
/// and not what dpb's `_connection.EnsureOpenAsync` gap needs.
fn is_local_function_node(kind: &str) -> bool {
    kind == "local_function_statement"
}

/// A C# anonymous function: `lambda_expression` covers both `x => ...` and
/// `(x, y) => ...` in the pinned tree-sitter-c-sharp grammar (0.23, which
/// unified what older grammars split into `simple_lambda_expression` /
/// `parenthesized_lambda_expression` — kept here too in case that ever
/// changes back); `anonymous_method_expression` is the legacy `delegate
/// (...) { ... }` form. Both lexically capture the enclosing `this` and
/// locals exactly like a nested block would — C# has no JS-style dynamic
/// `this` rebinding for any of these — so unlike `is_local_function_node`,
/// `walk_node` and `collect_statement_bindings` both recurse straight
/// through a node of this kind with the *same* `Context`/bindings map: it's
/// a nested scope, not a new symbol. See `collect_statement_bindings`'s
/// call site for how the lambda's own parameters get folded in so a
/// reference to one isn't mistaken for an outer name.
fn is_lambda_node(kind: &str) -> bool {
    matches!(
        kind,
        "anonymous_method_expression"
            | "lambda_expression"
            | "parenthesized_lambda_expression"
            | "simple_lambda_expression"
    )
}

/// Parameter names (+ inferred types, where explicitly annotated) bound by
/// a lambda/anonymous-method `parameters` field — either a `parameter_list`
/// (`(x, y) => ...`, shared shape with a method's own parameter list) or a
/// single unparenthesized `implicit_parameter` (`x => ...`, always
/// untyped). Folded into the *enclosing* method's `local_types` map by
/// `collect_statement_bindings` rather than given a scope of their own —
/// see `is_lambda_node`'s doc comment.
fn collect_lambda_parameter_bindings(
    params: Node<'_>,
    source: &str,
    bindings: &mut Vec<(String, LocalType)>,
) {
    if params.kind() == "implicit_parameter" {
        let name = node_text(params, source);
        if !name.is_empty() {
            bindings.push((name, LocalType::Other));
        }
        return;
    }
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
    ctx: &Context,
) {
    let mut cursor = node.walk();
    let mut bases = Vec::new();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "base_list" {
            continue;
        }
        bases.extend(base_list_types(child, source).into_iter().map(|b| {
            let args = closed_args(&b, node, source);
            (strip_type_args(&b), args)
        }));
    }
    if bases.is_empty() {
        return;
    }
    match kind {
        TypeKind::Class | TypeKind::Record => {
            let mut iter = bases.into_iter();
            if let Some((base, args)) = iter.next() {
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
                    import_candidates: type_ref_candidates(&base, ctx),
                    target_qualname: Some(base),
                    detail: args,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
            for (iface, args) in iter {
                output.edges.push(EdgeInput {
                    kind: "IMPLEMENTS".to_string(),
                    source_qualname: Some(qualname.to_string()),
                    import_candidates: type_ref_candidates(&iface, ctx),
                    target_qualname: Some(iface),
                    detail: args,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
        }
        TypeKind::Interface => {
            for (iface, args) in bases {
                output.edges.push(EdgeInput {
                    kind: "EXTENDS".to_string(),
                    source_qualname: Some(qualname.to_string()),
                    import_candidates: type_ref_candidates(&iface, ctx),
                    target_qualname: Some(iface),
                    detail: args,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
        }
        TypeKind::Struct | TypeKind::Enum => {
            for (iface, args) in bases {
                output.edges.push(EdgeInput {
                    kind: "IMPLEMENTS".to_string(),
                    source_qualname: Some(qualname.to_string()),
                    import_candidates: type_ref_candidates(&iface, ctx),
                    target_qualname: Some(iface),
                    detail: args,
                    evidence_snippet: None,
                    ..Default::default()
                });
            }
        }
    }
}

/// Attach the lookup scope to an unqualified interface receiver type (see
/// [`TypeScope`]); qualified names need none, and an alias is expanded.
fn with_type_scope(ty: String, ctx: &Context) -> ReceiverType {
    let ty = expand_type_alias(ty, ctx);
    let head = ty.split('<').next().unwrap_or(&ty);
    if head.contains('.') || !is_likely_interface_name(head) {
        return ReceiverType::Known(ty);
    }
    let enclosing = enclosing_scopes(ctx);
    let usings = using_scopes(ctx, &enclosing);
    let scope = TypeScope { enclosing, usings };
    if scope.encode().is_none() {
        return ReceiverType::Known(ty);
    }
    ReceiverType::Scoped { scope, ty }
}

/// `N1.IA<int>` -> `IA`: a receiver type without qualifier or arguments.
fn bare_type_name(ty: &str) -> &str {
    let head = ty.split('<').next().unwrap_or(ty);
    head.rsplit('.').next().unwrap_or(head)
}

/// Normalised closed type arguments of a base-list entry (`IA<Int32>` ->
/// `int`); `None` for a non-generic or open (`IA<T>`) one.
fn closed_args(text: &str, node: Node<'_>, source: &str) -> Option<String> {
    let identity = strip_open_args(explicit_interface_identity(text)?, node, source);
    let open = identity.find('<')?;
    let close = identity.rfind('>')?;
    (close > open + 1).then(|| identity[open + 1..close].to_string())
}

/// Enclosing namespaces of the current position, innermost first
/// (`A.B.C` -> `A.B.C`, `A.B`, `A`).
fn enclosing_namespaces(ctx: &Context) -> Vec<String> {
    let full = ctx.namespace_stack.join(".");
    let mut out = Vec::new();
    let mut ns = full.as_str();
    while !ns.is_empty() {
        out.push(ns.to_string());
        ns = ns.rsplit_once('.').map_or("", |(parent, _)| parent);
    }
    out
}

/// Scopes a type name is looked up in before the global namespace: the
/// enclosing types (their nested types), then the enclosing namespaces,
/// innermost first.
fn enclosing_scopes(ctx: &Context) -> Vec<String> {
    let namespaces = enclosing_namespaces(ctx);
    let base = namespaces.first().cloned().unwrap_or_default();
    let mut out = Vec::new();
    for depth in (1..=ctx.type_stack.len()).rev() {
        let types = ctx.type_stack[..depth].join(".");
        out.push(if base.is_empty() {
            types
        } else {
            format!("{base}.{types}")
        });
    }
    out.extend(namespaces);
    out
}

/// `using` namespaces not already an enclosing scope, in order.
fn using_scopes(ctx: &Context, enclosing: &[String]) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for ns in &ctx.imports.namespaces {
        if !enclosing.contains(ns) && !out.contains(ns) {
            out.push(ns.clone());
        }
    }
    out
}

/// Lookup guesses for a base-list type name in C# order (first hit wins in
/// the resolver): an alias, else `{scope}.{name}` for each enclosing scope,
/// then `name` itself (the global namespace), then each `using` namespace.
fn type_ref_candidates(name: &str, ctx: &Context) -> Vec<String> {
    let first = name.split('.').next().unwrap_or(name);
    if let Some(fqn) = ctx.imports.aliases.get(first) {
        return vec![format!("{fqn}{}", &name[first.len()..])];
    }
    let enclosing = enclosing_scopes(ctx);
    let mut out: Vec<String> = enclosing.iter().map(|s| format!("{s}.{name}")).collect();
    out.push(name.to_string());
    for ns in using_scopes(ctx, &enclosing) {
        let c = format!("{ns}.{name}");
        if !out.contains(&c) {
            out.push(c);
        }
    }
    out
}

/// The receiver type with a `using` alias in its first segment expanded
/// (`X.IA` with `using X = N1;` -> `N1.IA`; `A` with `using A = N1.IA;`).
fn expand_type_alias(ty: String, ctx: &Context) -> String {
    let head_end = ty.find('<').unwrap_or(ty.len());
    let head = &ty[..head_end];
    let first = head.split('.').next().unwrap_or(head);
    match ctx.imports.aliases.get(first) {
        Some(fqn) => format!("{fqn}{}", &ty[first.len()..]),
        None => ty,
    }
}

/// `IRepo<Order>` -> `IRepo`; `A<B>.C<D>` -> `A.C`. Type arguments never
/// take part in qualname resolution.
fn strip_type_args(name: &str) -> String {
    let mut depth = 0usize;
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        match ch {
            '<' => depth += 1,
            '>' if depth > 0 => depth -= 1,
            _ if depth == 0 => out.push(ch),
            _ => {}
        }
    }
    out.trim().to_string()
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

/// True when `ret` mentions a type parameter of the method itself or of any
/// enclosing generic type.
fn return_names_type_param(node: Node<'_>, ret: &str, source: &str) -> bool {
    let ident = |c: char| !(c.is_alphanumeric() || c == '_');
    let mut cur = Some(node);
    while let Some(n) = cur {
        let mut cursor = n.walk();
        let list = n.child_by_field_name("type_parameters").or_else(|| {
            n.named_children(&mut cursor)
                .find(|c| c.kind() == "type_parameter_list")
        });
        if let Some(tp) = list {
            let params = node_text(tp, source);
            if params
                .split(ident)
                .filter(|t| !t.is_empty())
                .any(|t| ret.split(ident).any(|r| r == t))
            {
                return true;
            }
        }
        cur = n.parent();
    }
    false
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
/// - A call result (`a.B().C()`) → the deferred return type of `B`.
/// - Anything deeper, or a chain rooted in something other than a bare
///   identifier/`this`/`base` (a cast, ...), → `Unresolved`
///   if the root is `this` or a tracked local, `NotTracked` otherwise.
fn infer_receiver_type(function_node: Node<'_>, source: &str, ctx: &Context) -> ReceiverType {
    match infer_receiver_type_raw(function_node, source, ctx) {
        ReceiverType::Known(ty) => {
            let ty = strip_open_args(ty, function_node, source);
            with_type_scope(ty, ctx)
        }
        other => other,
    }
}

fn infer_receiver_type_raw(function_node: Node<'_>, source: &str, ctx: &Context) -> ReceiverType {
    if function_node.kind() != "member_access_expression" {
        return ReceiverType::NotTracked;
    }
    let Some(object) = function_node.child_by_field_name("expression") else {
        return ReceiverType::NotTracked;
    };
    let (root, hops) = member_access_root(object);

    if root.kind() == "base" {
        if hops == 0 {
            return ctx.base_type.receiver();
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
            return attr_name
                .and_then(|name| {
                    type_at(
                        &name,
                        function_node.start_byte(),
                        ctx.class_attr_types.get(&name),
                        &ctx.assigns.attrs,
                    )
                })
                .map_or(ReceiverType::Unresolved, LocalType::receiver);
        }
        // ponytail: deeper chains (`this.a.b.Method()`) would need real
        // attribute-type inference across assignments — out of scope, same
        // ceiling as `python::infer_receiver_type`.
        return ReceiverType::Unresolved;
    }

    if hops == 0
        && matches!(
            root.kind(),
            "invocation_expression" | "await_expression" | "parenthesized_expression"
        )
    {
        // `a.B().C()`: the receiver is another call's return value.
        let names = Names {
            locals: &ctx.local_types,
            class_attrs: &ctx.class_attr_types,
            assigns: &ctx.assigns,
        };
        return pending_call(root, source, 0)
            .and_then(|call| call_marker(&call, &names, &ThisEnv::from_ctx(ctx)))
            .map_or(ReceiverType::Unresolved, ReceiverType::Deferred);
    }
    if root.kind() != "identifier" {
        // Chain rooted in a cast expression, subscript, etc. — not
        // inferable.
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
    let pos = function_node.start_byte();
    if let Some(local) = ctx.local_types.get(&root_name) {
        if hops == 0 {
            return type_at(&root_name, pos, Some(local), &ctx.assigns.locals)
                .map_or(ReceiverType::Unresolved, LocalType::receiver);
        }
        return ReceiverType::Unresolved;
    }
    if let Some(attr) = ctx.class_attr_types.get(&root_name) {
        if hops == 0 {
            return type_at(&root_name, pos, Some(attr), &ctx.assigns.attrs)
                .map_or(ReceiverType::Unresolved, LocalType::receiver);
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
/// (`Db::insert_edges` / `db::resolver::Resolver::resolve_import`) is what actually
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

impl ImportContext {
    /// Add the project's `global using` entries (see `scan_global_usings`)
    /// that the file does not already have.
    fn apply_globals(&mut self, globals: &[String]) {
        for entry in globals {
            match entry.split_once('=') {
                Some((alias, target)) => {
                    self.aliases
                        .entry(alias.to_string())
                        .or_insert_with(|| target.to_string());
                }
                None if !self.namespaces.contains(entry) => self.namespaces.push(entry.clone()),
                None => {}
            }
        }
    }
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
/// the DB layer's job (`db::resolver::Resolver::resolve_import`), which tries every
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
        .map(|t| classify_annotation_raw(&node_text(t, source)))
        .and_then(|ty| match ty {
            LocalType::Known(name) => Some(name),
            _ => None,
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
        if let (
            ReceiverType::Known(call_ty) | ReceiverType::Scoped { ty: call_ty, .. },
            Some(entry_ty),
        ) = (receiver_type, &entry.receiver_type)
            && bare_type_name(call_ty) != entry_ty
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
    // An *interface* type keeps the qualifier and closed generic arguments
    // it was written with (`N1.IA<int>`): dispatch (issues #173, #185) needs
    // the namespace to pick between same-named interfaces and the arguments
    // to pick a closed explicit impl. Other generics stay untracked
    // (`List<int>` must not bind to an unrelated project `List`).
    let head = text.split('<').next().unwrap_or(text);
    if is_likely_interface_name(head)
        && matches!(
            classify_annotation_raw(&strip_type_args(text)),
            LocalType::Known(_)
        )
        && let Some(identity) = explicit_interface_identity(text)
    {
        return LocalType::Known(identity);
    }
    classify_annotation_raw(text)
}

/// `classify_annotation` without generic-interface stripping: any generic
/// is `Other`. Used for extension-method receivers, whose applicability
/// check must not see a bare `IEnumerable` for `IEnumerable<T>`.
fn classify_annotation_raw(text: &str) -> LocalType {
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
fn classify_value_expr(value: Node<'_>, source: &str, method_returns: &MethodReturns) -> LocalType {
    // `(IA<int>)c` / `c as IA<int>`: the local has the cast's static type.
    match value.kind() {
        "cast_expression" => {
            if let Some(ty) = value.child_by_field_name("type") {
                return classify_annotation(&node_text(ty, source));
            }
        }
        "as_expression" => {
            if let Some(ty) = value.child_by_field_name("right") {
                return classify_annotation(&node_text(ty, source));
            }
        }
        "parenthesized_expression" => {
            if let Some(inner) = value.named_child(0) {
                return classify_value_expr(inner, source, method_returns);
            }
        }
        _ => {}
    }
    if value.kind() == "object_creation_expression"
        && let Some(type_node) = value.child_by_field_name("type")
    {
        return classify_annotation(&node_text(type_node, source));
    }
    if let Some(ret) = method_returns.call_return_type(value, source) {
        return classify_annotation(&ret);
    }
    match pending_call(value, source, 0) {
        // A same-file callee `call_return_type` couldn't type (`void`, a task
        // nobody awaited) stays untracked.
        Some(call)
            if call.recv == PendingRecv::This && method_returns.0.contains_key(&call.method) =>
        {
            LocalType::Other
        }
        Some(call) => LocalType::Call(call),
        None => LocalType::Other,
    }
}

/// The call (optionally awaited / `.ConfigureAwait(..)`) in `value` as a
/// `PendingCall` for `resolve_pending_calls`: `recv.Method(..)` with `recv` a
/// bare identifier or `this.field`, `M(..)` / `this.M(..)` / `base.M(..)`, or
/// a chain (`a.B().C()`, `(await a.B()).C()`). Anything else is `None`.
fn pending_call(value: Node<'_>, source: &str, depth: usize) -> Option<PendingCall> {
    if depth > MAX_DEFERRED_DEPTH {
        return None;
    }
    let (call, awaited) = unwrap_call(peel_parens(value), source)?;
    let function = call.child_by_field_name("function")?;
    let (recv, name) = match function.kind() {
        "identifier" | "generic_name" => (PendingRecv::This, function),
        "member_access_expression" => {
            let expr = peel_parens(function.child_by_field_name("expression")?);
            let recv = match expr.kind() {
                "identifier" => PendingRecv::Name(node_text(expr, source)),
                "this" => PendingRecv::This,
                "base" => PendingRecv::Base,
                "member_access_expression"
                    if expr.child_by_field_name("expression")?.kind() == "this" =>
                {
                    PendingRecv::Name(format!(
                        "this.{}",
                        node_text(expr.child_by_field_name("name")?, source)
                    ))
                }
                _ => PendingRecv::Call(Box::new(pending_call(expr, source, depth + 1)?)),
            };
            (recv, function.child_by_field_name("name")?)
        }
        _ => return None,
    };
    let method = node_text(name, source);
    let method = method.split('<').next().unwrap_or(&method).to_string();
    Some(PendingCall {
        pos: value.start_byte(),
        recv,
        method,
        awaited,
    })
}

fn peel_parens(mut node: Node<'_>) -> Node<'_> {
    while node.kind() == "parenthesized_expression"
        && let Some(inner) = node.named_child(0)
    {
        node = inner;
    }
    node
}

/// Names a `PendingCall` receiver can be bound to.
struct Names<'a> {
    locals: &'a HashMap<String, LocalType>,
    class_attrs: &'a HashMap<String, LocalType>,
    assigns: &'a ScopeAssigns,
}

impl Names<'_> {
    /// What `name` (a local or field; `this.field` when `this_field`) holds at
    /// byte `pos`.
    fn type_at(&self, name: &str, pos: usize, this_field: bool) -> Option<&LocalType> {
        if !this_field && self.locals.contains_key(name) {
            return type_at(name, pos, self.locals.get(name), &self.assigns.locals);
        }
        type_at(name, pos, self.class_attrs.get(name), &self.assigns.attrs)
    }
}

/// One plain assignment `name = value` that may change `name`'s type.
#[derive(Debug, Clone)]
struct Assign {
    /// End of the assignment: it holds from here on.
    pos: usize,
    /// Byte range of the block the assignment always runs in; a call
    /// outside it (after a branch) can't rely on it.
    region: (usize, usize),
    /// Loops around the assignment: a call earlier in one sees it too.
    loops: Vec<(usize, usize)>,
    ty: LocalType,
}

type Assigns = HashMap<String, Vec<Assign>>;

#[derive(Debug, Default)]
struct ScopeAssigns {
    locals: Assigns,
    attrs: Assigns,
}

static UNTRACKED: LocalType = LocalType::Other;

/// The type of `name` at byte `pos`: its latest assignment before `pos` when
/// that always runs before it, `base` (the declaration) when there is none,
/// and untracked when a branch or loop makes it ambiguous.
fn type_at<'a>(
    name: &str,
    pos: usize,
    base: Option<&'a LocalType>,
    assigns: &'a Assigns,
) -> Option<&'a LocalType> {
    let Some(events) = assigns.get(name) else {
        return base;
    };
    let in_range = |(start, end): (usize, usize)| start <= pos && pos < end;
    if events
        .iter()
        .any(|ev| ev.pos > pos && ev.loops.iter().any(|l| in_range(*l)))
    {
        return Some(&UNTRACKED);
    }
    match events.iter().rev().find(|ev| ev.pos <= pos) {
        None => base,
        Some(ev) if in_range(ev.region) => Some(&ev.ty),
        Some(_) => Some(&UNTRACKED),
    }
}

/// The deferred return for `call`: its receiver a bound `Known` type, a
/// bound `Deferred` local (nested in the new one), the enclosing / base
/// type, another call, or a name spelled like a static type (capitalised and
/// not bound at all). `None` when the receiver's type is unknown.
fn call_marker(call: &PendingCall, names: &Names<'_>, env: &ThisEnv) -> Option<DeferredReturn> {
    let inner = |ty: &LocalType| match ty {
        LocalType::Known(t) => Some(DeferredReturn::on_type(
            bare_type_name(t),
            &call.method,
            call.awaited,
            false,
        )),
        LocalType::Deferred(prev) => Some(DeferredReturn::on_call(
            prev.clone(),
            &call.method,
            call.awaited,
        )),
        _ => None,
    };
    let marker = match &call.recv {
        PendingRecv::Name(name) => {
            let bound = match name.strip_prefix("this.") {
                Some(field) => names.type_at(field, call.pos, true),
                None => names.type_at(name, call.pos, false),
            };
            match bound {
                Some(ty) => inner(ty)?,
                // Possibly a static type name -- but also possibly an
                // inherited property; the resolver only accepts a `static`
                // callee for this shape.
                None if name.starts_with(|c: char| c.is_ascii_uppercase()) => {
                    match classify_type_name(name) {
                        LocalType::Known(t) => {
                            DeferredReturn::on_type(&t, &call.method, call.awaited, true)
                        }
                        _ => return None,
                    }
                }
                None => return None,
            }
        }
        PendingRecv::This => inner(&LocalType::Known(env.this_type.clone()?))?,
        PendingRecv::Base => inner(&LocalType::Known(env.base_type.clone()?))?,
        PendingRecv::Call(prev) => {
            DeferredReturn::on_call(call_marker(prev, names, env)?, &call.method, call.awaited)
        }
    };
    (marker.depth() <= MAX_DEFERRED_DEPTH).then_some(marker)
}

/// Finish every pending call binding -- declarations and assignments -- as a
/// `Deferred` marker for the resolver (or `Other` when its receiver's type
/// is unknown). A pending call on another pending name (`var a = F(); var b =
/// a.G();`) waits for that one, a round at a time, up to `MAX_DEFERRED_DEPTH`
/// deep.
fn resolve_pending_calls(
    locals: &mut HashMap<String, LocalType>,
    class_attr_types: &HashMap<String, LocalType>,
    assigns: &mut ScopeAssigns,
    env: &ThisEnv,
) {
    let pending = |ty: &LocalType| matches!(ty, LocalType::Call(_));
    for _ in 0..=MAX_DEFERRED_DEPTH {
        let snapshot_locals = locals.clone();
        let snapshot_assigns = ScopeAssigns {
            locals: assigns.locals.clone(),
            attrs: assigns.attrs.clone(),
        };
        let names = Names {
            locals: &snapshot_locals,
            class_attrs: class_attr_types,
            assigns: &snapshot_assigns,
        };
        let mut progressed = false;
        let mut finish = |ty: &mut LocalType| {
            let LocalType::Call(call) = &*ty else {
                return;
            };
            let waits = {
                let mut root = call;
                while let PendingRecv::Call(inner) = &root.recv {
                    root = inner;
                }
                matches!(&root.recv, PendingRecv::Name(n)
                    if names.type_at(n.strip_prefix("this.").unwrap_or(n), root.pos, n.starts_with("this."))
                        .is_some_and(pending))
            };
            if waits {
                return;
            }
            *ty = call_marker(call, &names, env).map_or(LocalType::Other, LocalType::Deferred);
            progressed = true;
        };
        for ty in locals.values_mut() {
            finish(ty);
        }
        for ev in assigns
            .locals
            .values_mut()
            .chain(assigns.attrs.values_mut())
            .flatten()
        {
            finish(&mut ev.ty);
        }
        if !progressed {
            break;
        }
    }
    // Still pending: an unresolvable cycle or a chain past the depth cap.
    for ty in locals.values_mut() {
        if pending(ty) {
            *ty = LocalType::Other;
        }
    }
    for ev in assigns
        .locals
        .values_mut()
        .chain(assigns.attrs.values_mut())
        .flatten()
    {
        if pending(&ev.ty) {
            ev.ty = LocalType::Other;
        }
    }
}

/// Every identifier under a deconstruction target.
fn collect_identifiers(node: Node<'_>, source: &str, out: &mut Vec<String>) {
    if node.kind() == "identifier" {
        out.push(node_text(node, source));
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_identifiers(child, source, out);
    }
}

/// Every assignment to a name in `body`, keyed by name, sorted by position:
/// to a local declared in `locals`, else to a field.
fn collect_assignments(
    body: Node<'_>,
    source: &str,
    method_returns: &MethodReturns,
    locals: &HashMap<String, LocalType>,
) -> ScopeAssigns {
    fn walk(
        node: Node<'_>,
        source: &str,
        returns: &MethodReturns,
        locals: &HashMap<String, LocalType>,
        loops: &mut Vec<(usize, usize)>,
        out: &mut ScopeAssigns,
    ) {
        let is_loop = matches!(
            node.kind(),
            "for_statement" | "foreach_statement" | "while_statement" | "do_statement"
        );
        if is_loop {
            loops.push((node.start_byte(), node.end_byte()));
        }
        if node.kind() == "assignment_expression"
            && let (Some(left), Some(right)) = (
                node.child_by_field_name("left"),
                node.child_by_field_name("right"),
            )
        {
            let plain = node
                .child_by_field_name("operator")
                .is_some_and(|op| node_text(op, source) == "=");
            // Only `x = <typed value>` gives `x` a type; any other write
            // (`??=`, `+=`, a deconstruction, an untypeable value) leaves
            // it unknown from here on.
            let ty = if plain && left.kind() != "tuple_expression" {
                match classify_value_expr(right, source, returns) {
                    ty @ (LocalType::Known(_) | LocalType::Call(_)) => ty,
                    _ => LocalType::Other,
                }
            } else {
                LocalType::Other
            };
            let mut targets: Vec<String> = Vec::new();
            match left.kind() {
                "identifier" => targets.push(node_text(left, source)),
                "member_access_expression"
                    if left.child_by_field_name("expression").map(|e| e.kind()) == Some("this") =>
                {
                    targets.extend(
                        left.child_by_field_name("name")
                            .map(|n| node_text(n, source)),
                    );
                }
                "tuple_expression" => collect_identifiers(left, source, &mut targets),
                _ => {}
            }
            // Always runs before what follows in its block only as a whole
            // statement of that block; anything else (a branch's lone
            // statement, a condition, a lambda body) just poisons.
            let stmt = node.parent().filter(|p| p.kind() == "expression_statement");
            let region = match stmt.and_then(|s| s.parent()) {
                Some(block)
                    if matches!(
                        block.kind(),
                        "block" | "switch_section" | "global_statement" | "compilation_unit"
                    ) =>
                {
                    (block.start_byte(), block.end_byte())
                }
                _ => (node.start_byte(), node.end_byte()),
            };
            for name in targets {
                let assigns = if locals.contains_key(&name) {
                    &mut out.locals
                } else {
                    &mut out.attrs
                };
                assigns.entry(name).or_default().push(Assign {
                    pos: node.end_byte(),
                    region,
                    loops: loops.clone(),
                    ty: ty.clone(),
                });
            }
        }
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            walk(child, source, returns, locals, loops, out);
        }
        if is_loop {
            loops.pop();
        }
    }
    let mut out = ScopeAssigns::default();
    walk(
        body,
        source,
        method_returns,
        locals,
        &mut Vec::new(),
        &mut out,
    );
    for events in out.locals.values_mut().chain(out.attrs.values_mut()) {
        events.sort_by_key(|ev| ev.pos);
    }
    out
}

/// Declared return types (raw text) of every method / local function in a
/// file, keyed by bare name. A name declared more than once with different
/// return types is dropped, as is a generic method whose return type names
/// one of its own type parameters: ambiguous means untracked, never a
/// wrong type.
#[derive(Debug, Default)]
struct MethodReturns(HashMap<String, String>);

impl MethodReturns {
    fn collect(root: Node<'_>, source: &str) -> Self {
        fn walk(node: Node<'_>, source: &str, out: &mut HashMap<String, Option<String>>) {
            if matches!(
                node.kind(),
                "method_declaration" | "local_function_statement"
            ) && let (Some(name), Some(ret)) = (
                node.child_by_field_name("name"),
                node.child_by_field_name("returns")
                    .or_else(|| node.child_by_field_name("type")),
            ) {
                let name = node_text(name, source);
                let ret = node_text(ret, source).trim().to_string();
                let names_type_param = return_names_type_param(node, &ret, source);
                match out.get(&name) {
                    _ if names_type_param => {
                        out.insert(name, None);
                    }
                    Some(Some(prev)) if *prev == ret => {}
                    Some(_) => {
                        out.insert(name, None);
                    }
                    None => {
                        out.insert(name, Some(ret));
                    }
                }
            }
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                walk(child, source, out);
            }
        }
        let mut out = HashMap::new();
        walk(root, source, &mut out);
        Self(
            out.into_iter()
                .filter_map(|(k, v)| v.map(|v| (k, v)))
                .collect(),
        )
    }

    /// The declared return type text of a bare / `this.` call (optionally
    /// awaited, optionally `.ConfigureAwait(..)`) to a same-file method. An
    /// awaited call unwraps `Task<T>` / `ValueTask<T>`; a non-awaited
    /// `Task<T>` (or bare `Task`) yields `None`, as does `void` and any
    /// callee not recorded.
    fn call_return_type(&self, value: Node<'_>, source: &str) -> Option<String> {
        let (call, awaited) = unwrap_call(value, source)?;
        let function = call.child_by_field_name("function")?;
        let name_node = match function.kind() {
            "identifier" | "generic_name" => function,
            "member_access_expression"
                if function.child_by_field_name("expression")?.kind() == "this" =>
            {
                function.child_by_field_name("name")?
            }
            _ => return None,
        };
        let mut name = node_text(name_node, source);
        if let Some(idx) = name.find('<') {
            name.truncate(idx);
        }
        unwrap_return(self.0.get(&name)?, awaited)
    }
}

/// The invocation inside `value` (peeling `await` and a trailing
/// `.ConfigureAwait(..)`) and whether it was awaited.
fn unwrap_call<'t>(value: Node<'t>, source: &str) -> Option<(Node<'t>, bool)> {
    let (mut call, awaited) = if value.kind() == "await_expression" {
        (value.named_child(0)?, true)
    } else {
        (value, false)
    };
    if awaited
        && call.kind() == "invocation_expression"
        && let Some(f) = call.child_by_field_name("function")
        && f.kind() == "member_access_expression"
        && f.child_by_field_name("name")
            .is_some_and(|n| node_text(n, source) == "ConfigureAwait")
    {
        call = f.child_by_field_name("expression")?;
    }
    (call.kind() == "invocation_expression").then_some((call, awaited))
}

/// A declared return type's usable text: an awaited `Task<T>` /
/// `ValueTask<T>` unwraps to `T`; a non-awaited task, a bare `Task` and
/// `void` yield `None`.
fn unwrap_return(ret: &str, awaited: bool) -> Option<String> {
    let ret = ret.trim();
    let wrapped = ["Task<", "ValueTask<"]
        .iter()
        .find_map(|p| ret.strip_prefix(p).and_then(|r| r.strip_suffix('>')));
    let is_task_like = wrapped.is_some() || ["Task", "ValueTask", "void"].contains(&ret);
    match (awaited, wrapped) {
        (true, Some(inner)) => Some(inner.trim().to_string()),
        _ if is_task_like => None,
        _ => Some(ret.to_string()),
    }
}

/// `LanguageProfile::deferred_receiver`: the type a deferred call returns
/// (`""` when it can't be told, so the call binds nothing).
fn resolve_deferred(
    marker: &DeferredMarker,
    index: &dyn DeclarationIndex,
) -> Result<Option<Option<String>>> {
    match marker {
        DeferredMarker::Argument(arg) => {
            Ok(Some(Some(argument_type(arg, index)?.unwrap_or_default())))
        }
        DeferredMarker::Return(call) => Ok(Some(Some(
            receiver_type(call, index, 0)?.unwrap_or_default(),
        ))),
        DeferredMarker::Rust(_) => Ok(None),
    }
}

/// The repo type a `new(..)` argument constructs: the declared type of the
/// parameter it is passed for, shared by every arity-admitted overload of the
/// callee (differing or unreadable parameters -> `None`, never a guess) and
/// naming a repo type (which rules out a type parameter).
fn argument_type(arg: &DeferredArgument, index: &dyn DeclarationIndex) -> Result<Option<String>> {
    let (ty, method) = match arg.callee.strip_suffix("..ctor") {
        Some(ty) => (ty, ".ctor"),
        None => match arg.callee.rsplit_once('.') {
            Some(split) => split,
            None => return Ok(None),
        },
    };
    let mut found: Option<String> = None;
    for decl in index.declarations(DeclarationQuery::Member { ty, method })? {
        let Some(sig) = decl.signature.as_deref() else {
            return Ok(None);
        };
        if !crate::db::resolver::admits_arg_count(arg.arg_count, sig) {
            continue;
        }
        let Some(param) = parameter_type_from_signature(sig, arg.index, arg.name.as_deref()) else {
            return Ok(None);
        };
        match &found {
            Some(prev) if *prev != param => return Ok(None),
            _ => found = Some(param),
        }
    }
    let Some(param) = found else {
        return Ok(None);
    };
    Ok(index.is_repo_type(&param)?.then_some(param))
}

/// The repo type `call` returns: the return type shared by every method it
/// may reach (overloads, same-named types in other namespaces), which must
/// name a repo type -- also ruling out a generic type parameter such as `T`.
fn receiver_type(
    call: &DeferredReturn,
    index: &dyn DeclarationIndex,
    depth: usize,
) -> Result<Option<String>> {
    let ret = reachable(call, index, depth, |decl, awaited| {
        Ok(decl
            .signature
            .as_deref()
            .and_then(|sig| receiver_from_signature(sig, awaited)))
    })?;
    match ret {
        Some(ret) if index.is_repo_type(&ret)? => Ok(Some(ret)),
        _ => Ok(None),
    }
}

/// `map` of every method a deferred call may reach -- the receiver type's
/// own, else its nearest ancestors' -- when they all agree on one `Some`
/// value. `None` for no method, a disagreement, a `map` miss, or a
/// `static_only` call reaching a non-`static` method.
fn reachable<T: PartialEq>(
    call: &DeferredReturn,
    index: &dyn DeclarationIndex,
    depth: usize,
    map: impl Fn(&Declaration, bool) -> Result<Option<T>>,
) -> Result<Option<T>> {
    // A nested call is the receiver's own deferred call (`a.B().C()`).
    let ty = match &call.base {
        DeferredBase::Type(ty) => ty.clone(),
        DeferredBase::Call(_) if depth >= MAX_DEFERRED_DEPTH => return Ok(None),
        DeferredBase::Call(inner) => match receiver_type(inner, index, depth + 1)? {
            Some(ty) => ty,
            None => return Ok(None),
        },
    };
    let mut found: Option<T> = None;
    for decl in index.inherited_members(&ty, &call.method)? {
        let is_static = decl
            .visibility
            .as_deref()
            .is_some_and(|v| v.split_whitespace().any(|m| m == "static"));
        let value = if is_static || !call.static_only {
            map(&decl, call.awaited)?
        } else {
            None
        };
        let Some(value) = value else {
            return Ok(None);
        };
        match &found {
            Some(prev) if *prev != value => return Ok(None),
            _ => found = Some(value),
        }
    }
    Ok(found)
}

/// `LanguageProfile::deferred_rpc`: the `RPC_CALL` edges of a call of
/// `method` whose receiver is the return value of a callee returning a
/// generated gRPC client -- one per candidate package, taken from the
/// imports of the callee's file, where the client type is written.
fn deferred_rpc_calls(
    marker: &DeferredMarker,
    method: &str,
    index: &dyn DeclarationIndex,
) -> Result<Option<Vec<RpcCallEdge>>> {
    let DeferredMarker::Return(call) = marker else {
        return Ok(None);
    };
    reachable(call, index, 0, |decl, awaited| {
        let Some(signature) = decl.signature.as_deref() else {
            return Ok(None);
        };
        let Some(client) = client_of_signature(signature, awaited) else {
            return Ok(None);
        };
        let imports = index.imports_in_scope(decl)?;
        Ok(Some(grpc_edges(client, method, &imports)))
    })
    .map(|edges| {
        edges.map(|e| {
            e.into_iter()
                .map(|(t, d)| RpcCallEdge {
                    target_qualname: t,
                    detail: d,
                })
                .collect()
        })
    })
}

/// `(service, prefix)` of the generated gRPC client type a method with this
/// indexed `signature` returns.
fn client_of_signature(signature: &str, awaited: bool) -> Option<(String, Option<String>)> {
    let ret = unwrap_return(signature.rsplit_once(" -> ")?.1, awaited)?;
    split_client_service_and_prefix(ret.trim())
}

/// `(target_qualname, detail)` of an `RPC_CALL` edge per candidate package.
fn grpc_edges(
    client: (String, Option<String>),
    method: &str,
    scope: &ScopeImports,
) -> Vec<(String, String)> {
    let Some(rpc) = normalize_grpc_method_name(method.split('<').next().unwrap_or(method)) else {
        return Vec::new();
    };
    let imports = ImportContext {
        namespaces: scope.namespaces.clone(),
        aliases: scope.aliases.clone(),
    };
    build_grpc_call_edges(&[client], &rpc, "", &None, 0, 0, &imports)
        .into_iter()
        .filter_map(|e| Some((e.target_qualname?, e.detail?)))
        .collect()
}

/// The receiver type name a call to a method with this indexed `signature`
/// (`(params) -> Ret`) yields, for the resolver's `ReceiverType::Deferred`.
/// `None` when the signature has no return type or it isn't a plain
/// non-builtin type name (the resolver separately requires a repo type of
/// that name, which rules out a generic type parameter).
fn receiver_from_signature(signature: &str, awaited: bool) -> Option<String> {
    let ret = unwrap_return(signature.rsplit_once(" -> ")?.1, awaited)?;
    match classify_annotation(&ret) {
        LocalType::Known(name) => Some(bare_type_name(&name).to_string()),
        _ => None,
    }
}

/// The (non-builtin) type name of the parameter at `index`, or named `name`,
/// in an indexed signature `(params) -> Ret`. `None` for `params`, `this`,
/// `ref`/`out`/`in` parameters, a missing parameter, or a non-plain type.
fn parameter_type_from_signature(
    signature: &str,
    index: usize,
    name: Option<&str>,
) -> Option<String> {
    let params = crate::db::resolver::parameter_list(signature.strip_prefix('(')?)?;
    let parsed: Vec<(String, String)> = crate::db::resolver::split_top_level(params)
        .into_iter()
        .filter(|p| !p.trim().is_empty())
        .map(|param| {
            let param = param.trim();
            let head = crate::db::resolver::top_level_chars(param)
                .find(|&(_, c)| c == '=')
                .map_or(param, |(at, _)| &param[..at])
                .trim();
            let (ty, pname) = head.rsplit_once(char::is_whitespace).unwrap_or((head, ""));
            (ty.trim().to_string(), pname.trim().to_string())
        })
        .collect();
    let (ty, _) = match name {
        Some(name) => parsed.iter().find(|(_, pname)| pname == name)?,
        None => parsed.get(index)?,
    };
    if ["params ", "this ", "ref ", "out ", "in "]
        .iter()
        .any(|m| ty.starts_with(m))
    {
        return None;
    }
    match classify_annotation(ty) {
        LocalType::Known(name) => Some(name),
        _ => None,
    }
}

/// Element types of a tuple return type text (`(A, B)` / `(A a, B b)`),
/// `None` per element when it isn't a plain type. `None` overall when
/// `text` isn't a tuple.
fn tuple_element_types(text: &str) -> Option<Vec<String>> {
    let inner = text.trim().strip_prefix('(')?.strip_suffix(')')?;
    Some(
        split_respecting_brackets(inner)
            .into_iter()
            .map(|el| {
                let el = el.trim();
                match el.rsplit_once(char::is_whitespace) {
                    Some((ty, name))
                        if !name.is_empty()
                            && name.chars().all(|c| c.is_alphanumeric() || c == '_') =>
                    {
                        ty.trim().to_string()
                    }
                    _ => el.to_string(),
                }
            })
            .collect(),
    )
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
fn infer_local_types(
    function_node: Node<'_>,
    source: &str,
    ctx: &Context,
) -> (HashMap<String, LocalType>, ScopeAssigns) {
    let method_returns = &*ctx.method_returns;
    let mut bindings: Vec<(String, LocalType)> = Vec::new();
    let mut raw = RawTypes {
        locals: HashMap::new(),
        class: &ctx.class_attr_raw,
    };
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
            if let Some(t) = param.child_by_field_name("type") {
                raw.record(&name, Some(node_text(t, source)));
            }
            bindings.push((name, ty));
        }
    }
    if let Some(body) = function_node.child_by_field_name("body") {
        collect_statement_bindings(body, source, method_returns, &mut raw, &mut bindings);
    }
    let mut locals = bindings_to_local_types(bindings);
    let mut assigns = function_node
        .child_by_field_name("body")
        .map(|body| collect_assignments(body, source, method_returns, &locals))
        .unwrap_or_default();
    resolve_pending_calls(
        &mut locals,
        &ctx.class_attr_types,
        &mut assigns,
        &ThisEnv::from_ctx(ctx),
    );
    (locals, assigns)
}

/// `infer_local_types` for a compilation unit's top-level statements, which
/// share one scope (their local functions are boundaries, as in a method).
fn infer_global_local_types(
    root: Node<'_>,
    source: &str,
    ctx: &Context,
) -> (HashMap<String, LocalType>, ScopeAssigns) {
    let mut bindings: Vec<(String, LocalType)> = Vec::new();
    let mut raw = RawTypes {
        locals: HashMap::new(),
        class: &ctx.class_attr_raw,
    };
    let mut cursor = root.walk();
    for child in root.named_children(&mut cursor) {
        if child.kind() == "global_statement" {
            collect_statement_bindings(child, source, &ctx.method_returns, &mut raw, &mut bindings);
        }
    }
    let mut locals = bindings_to_local_types(bindings);
    let mut assigns = ScopeAssigns::default();
    let mut cursor = root.walk();
    for child in root.named_children(&mut cursor) {
        if child.kind() == "global_statement" {
            let found = collect_assignments(child, source, &ctx.method_returns, &locals);
            for (from, to) in [
                (found.locals, &mut assigns.locals),
                (found.attrs, &mut assigns.attrs),
            ] {
                for (name, events) in from {
                    to.entry(name).or_default().extend(events);
                }
            }
        }
    }
    resolve_pending_calls(
        &mut locals,
        &ctx.class_attr_types,
        &mut assigns,
        &ThisEnv::default(),
    );
    (locals, assigns)
}

/// Declared type text of names bound in the current method (`None` once a
/// name is bound twice), falling back to the enclosing type's fields.
struct RawTypes<'a> {
    locals: HashMap<String, Option<String>>,
    class: &'a HashMap<String, String>,
}

impl RawTypes<'_> {
    fn record(&mut self, name: &str, text: Option<String>) {
        self.locals
            .entry(name.to_string())
            .and_modify(|t| *t = None)
            .or_insert(text);
    }
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
/// single method/constructor body, stopping at a nested local-function
/// boundary (its own locals are a different scope entirely — see
/// `Context::local_types`'s doc comment; reuses `is_local_function_node`,
/// the same boundary `walk_node` itself stops at). A lambda/anonymous
/// method is *not* a boundary here — see `is_lambda_node`'s doc comment —
/// so a call inside one is walked with the *enclosing* method's
/// `local_types`, and the lambda's own parameters are folded into that same
/// map below (mirrors `python::collect_statement_bindings`'s `"lambda"`
/// arm) so a reference to one of them isn't mistaken for an outer name and
/// misattributed to whatever the enclosing scope happens to bind that name
/// to.
///
/// `var (a, b) = Method(..)` is bound via `bind_tuple_pattern`.
///
/// `foreach (var (k, v) in expr)` is bound via `bind_foreach_deconstruction`.
fn collect_statement_bindings(
    node: Node<'_>,
    source: &str,
    method_returns: &MethodReturns,
    raw: &mut RawTypes<'_>,
    bindings: &mut Vec<(String, LocalType)>,
) {
    if is_local_function_node(node.kind()) {
        return;
    }
    if is_lambda_node(node.kind())
        && let Some(params) = node.child_by_field_name("parameters")
    {
        collect_lambda_parameter_bindings(params, source, bindings);
        // No `return`: still recurse into children below (the body may
        // declare further locals, or contain a nested lambda whose own
        // parameters also need folding in).
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
                if is_var
                    && let Some(pattern) =
                        child.named_child(0).filter(|n| n.kind() == "tuple_pattern")
                {
                    bind_tuple_pattern(pattern, child, source, method_returns, bindings);
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
                let value = variable_declarator_value(child);
                let ty = if is_var {
                    value
                        .map(|v| classify_value_expr(v, source, method_returns))
                        .unwrap_or(LocalType::Other)
                } else {
                    classify_annotation(&node_text(type_node.expect("checked above"), source))
                };
                let text = match (is_var, value) {
                    (false, _) => type_node.map(|t| node_text(t, source)),
                    (true, Some(v)) if v.kind() == "object_creation_expression" => {
                        v.child_by_field_name("type").map(|t| node_text(t, source))
                    }
                    (true, Some(v)) => method_returns.call_return_type(v, source),
                    (true, None) => None,
                };
                raw.record(&name, text);
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
        "foreach_statement"
            if node
                .child_by_field_name("left")
                .is_some_and(|l| l.kind() != "identifier") =>
        {
            bind_foreach_deconstruction(node, source, method_returns, raw, bindings);
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
        collect_statement_bindings(child, source, method_returns, raw, bindings);
    }
}

/// Bind the names of `foreach (var (k, v) in expr)`. Each gets the matching
/// element of `expr`'s element type when that is a tuple (`IEnumerable<(A,
/// B)>`, `List<(A, B)>`, `(A, B)[]`) or a dictionary's key/value pair;
/// otherwise -- and for any explicit-typed, nested or discard shape -- the
/// names are bound `Other` so they still shadow a same-named field.
fn bind_foreach_deconstruction(
    node: Node<'_>,
    source: &str,
    method_returns: &MethodReturns,
    raw: &RawTypes<'_>,
    bindings: &mut Vec<(String, LocalType)>,
) {
    let Some(left) = node.child_by_field_name("left") else {
        return;
    };
    let implicit = node
        .child_by_field_name("type")
        .is_some_and(|t| t.kind() == "implicit_type");
    let elements = node
        .child_by_field_name("right")
        .filter(|_| implicit && left.kind() == "tuple_pattern")
        .and_then(|right| {
            let text = match right.kind() {
                "identifier" => {
                    let name = node_text(right, source);
                    if raw.locals.contains_key(&name) {
                        raw.locals.get(&name)?.clone()
                    } else if bindings.iter().any(|(n, _)| *n == name) {
                        None
                    } else {
                        raw.class.get(&name).cloned()
                    }
                }
                _ => method_returns.call_return_type(right, source),
            }?;
            foreach_element_types(&text)
        });
    if let Some(types) = elements.filter(|t| t.len() == left.named_child_count()) {
        let mut cursor = left.walk();
        for (el, ty) in left.named_children(&mut cursor).zip(types) {
            if el.kind() == "identifier" {
                bindings.push((node_text(el, source), classify_annotation(&ty)));
            } else {
                poison_identifiers(el, source, bindings);
            }
        }
    } else {
        poison_identifiers(left, source, bindings);
    }
}

/// Bind every identifier under `node` (a pattern / declaration expression)
/// to `Other`.
fn poison_identifiers(node: Node<'_>, source: &str, bindings: &mut Vec<(String, LocalType)>) {
    if node.kind() == "identifier" {
        bindings.push((node_text(node, source), LocalType::Other));
        return;
    }
    if node.kind() == "declaration_expression"
        && let Some(name) = node.child_by_field_name("name")
    {
        bindings.push((node_text(name, source), LocalType::Other));
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        poison_identifiers(child, source, bindings);
    }
}

/// The deconstruction element types of one item of a collection type text:
/// `List<(A, B)>` / `IEnumerable<(A x, B y)>` / `(A, B)[]` give `[A, B]`,
/// `Dictionary<K, V>` gives `[K, V]`; `None` for anything else.
fn foreach_element_types(text: &str) -> Option<Vec<String>> {
    let text = text.trim().trim_end_matches('?');
    if let Some(elem) = text.strip_suffix("[]") {
        return tuple_element_types(elem);
    }
    let (name, rest) = text.split_once('<')?;
    let inner = rest.strip_suffix('>')?;
    let name = name.rsplit('.').next()?.trim();
    // A tuple's own commas aren't bracket-nested, so only the dictionary
    // shape splits its arguments (any tuple key/value then fails to match).
    match name {
        "IEnumerable"
        | "IAsyncEnumerable"
        | "List"
        | "IList"
        | "ICollection"
        | "IReadOnlyList"
        | "IReadOnlyCollection"
        | "HashSet"
        | "ISet"
        | "Queue"
        | "Stack"
        | "Collection"
        | "ImmutableArray"
        | "ImmutableList" => tuple_element_types(inner),
        "Dictionary"
        | "IDictionary"
        | "IReadOnlyDictionary"
        | "SortedDictionary"
        | "ConcurrentDictionary"
        | "ImmutableDictionary" => match split_respecting_brackets(inner).as_slice() {
            [k, v] => Some(vec![k.trim().to_string(), v.trim().to_string()]),
            _ => None,
        },
        _ => None,
    }
}

/// Bind the names of `var (a, b) = Method(..)`: each gets the matching
/// element of the callee's tuple return type when determinable, else
/// `Other` (still bound so it shadows a same-named field). Discards and
/// nested patterns are skipped / `Other`.
fn bind_tuple_pattern(
    pattern: Node<'_>,
    declarator: Node<'_>,
    source: &str,
    method_returns: &MethodReturns,
    bindings: &mut Vec<(String, LocalType)>,
) {
    let elements = variable_declarator_value(declarator)
        .and_then(|v| method_returns.call_return_type(v, source))
        .and_then(|ret| tuple_element_types(&ret));
    let mut cursor = pattern.walk();
    for (idx, el) in pattern.named_children(&mut cursor).enumerate() {
        if el.kind() != "identifier" {
            continue;
        }
        let ty = match &elements {
            Some(types) if types.len() == pattern.named_child_count() => types
                .get(idx)
                .map(|t| classify_annotation(t))
                .unwrap_or(LocalType::Other),
            _ => LocalType::Other,
        };
        bindings.push((node_text(el, source), ty));
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
    class_level_typed_members(class_body, source)
        .into_iter()
        .map(|(name, ty)| (name, classify_annotation(&ty)))
        .collect()
}

/// The same fields/properties as `collect_class_level_attr_types`, keyed to
/// their declared type text (`List<(A, B)>`, ...).
fn collect_class_level_attr_type_texts(
    class_body: Node<'_>,
    source: &str,
) -> HashMap<String, String> {
    class_level_typed_members(class_body, source)
        .into_iter()
        .collect()
}

/// `(name, declared type text)` of every explicitly typed field / property
/// declared directly in a type's body.
fn class_level_typed_members(class_body: Node<'_>, source: &str) -> Vec<(String, String)> {
    let mut result = Vec::new();
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
                    let ty = node_text(type_node, source);
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
                        if !name.is_empty() {
                            result.push((name, ty.clone()));
                        }
                    }
                }
            }
            "property_declaration" => {
                let (Some(name_node), Some(type_node)) = (
                    member.child_by_field_name("name"),
                    member.child_by_field_name("type"),
                ) else {
                    continue;
                };
                let name = node_text(name_node, source);
                if !name.is_empty() {
                    result.push((name, node_text(type_node, source)));
                }
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
    method_returns: &MethodReturns,
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
                    collect_grpc_clients_from_declaration(
                        decl,
                        source,
                        method_returns,
                        &mut result,
                    );
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
    #[test]
    fn scan_global_usings_reads_namespaces_and_aliases() {
        let source = "global using N1;\nglobal using  Alias = N2.Type ; // c\nglobal using static X.Y;\nusing Local;\nnamespace A {}\n";
        assert_eq!(
            scan_global_usings(source),
            ["N1".to_string(), "Alias=N2.Type".to_string()]
        );
    }

    #[test]
    fn normalize_type_args_canonicalises_aliases_nullables_and_nesting() {
        use super::normalize_type_args as n;
        assert_eq!(n("Int32"), "int");
        assert_eq!(n("System.String"), "string");
        assert_eq!(n("String?"), "string");
        assert_eq!(n("object?"), "object");
        assert_eq!(n("int?"), "int?");
        assert_eq!(n("Int64 , System.Boolean"), "long,bool");
        assert_eq!(
            n("Dictionary<String, List<Int32>>"),
            "Dictionary<string,List<int>>"
        );
        assert_eq!(n("System.Guid"), "System.Guid");
    }

    #[test]
    fn explicit_interface_identity_keeps_closed_generics_and_namespace() {
        use super::explicit_interface_identity as f;
        assert_eq!(f("Outer<T>.IA.").as_deref(), Some("Outer.IA"));
        assert_eq!(f("N.IA<T>.").as_deref(), Some("N.IA<T>"));
        assert_eq!(f("IA<int>.").as_deref(), Some("IA<int>"));
        assert_eq!(
            f("IA<Dictionary<K, V>>.").as_deref(),
            Some("IA<Dictionary<K,V>>")
        );
        assert_eq!(f("global::N1.IA.").as_deref(), Some("N1.IA"));
        assert_eq!(f("IA.").as_deref(), Some("IA"));
    }

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
    fn grpc_impl_requires_public_override() {
        // Regression for #125: a private helper (including private static)
        // declared alongside a real RPC method in a `*ServiceBase` subclass
        // must not get an RPC_IMPL edge -- only `public override` methods
        // are actual gRPC method implementations; everything else is just a
        // helper that happens to live in the same class.
        let source = r#"
using Inventory.V1;

namespace Dpb.DataMgr.Catalog.Grpc {
  internal class InventoryServiceImpl : InventoryService.InventoryServiceBase {
    public override Task<GetInventoryResponse> GetInventory(
        GetInventoryRequest request, ServerCallContext context) {
      return MapStatus(request);
    }

    private Task<GetInventoryResponse> MapStatus(GetInventoryRequest request) {
      return null;
    }

    private static string ToRpcException(string message) {
      return message;
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
        assert_eq!(
            impls.len(),
            1,
            "only the public override method should get an RPC_IMPL edge, got {:?}",
            impls.iter().map(|e| &e.source_qualname).collect::<Vec<_>>()
        );
        assert!(impls.iter().any(|edge| edge.target_qualname.as_deref()
            == Some("/inventory.v1.inventoryservice/getinventory")));
        assert!(
            !impls.iter().any(|edge| edge
                .source_qualname
                .as_deref()
                .is_some_and(|q| q.ends_with("MapStatus") || q.ends_with("ToRpcException"))),
            "private helpers must not get RPC_IMPL edges, got {:?}",
            impls.iter().map(|e| &e.source_qualname).collect::<Vec<_>>()
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
    fn grpc_call_resolves_client_field_regardless_of_scan_order() {
        // Regression test for the exact defect measured on dpb: a test
        // fixture ("Scope") class exposes its gRPC client through a field
        // (`Client`, itself constructed elsewhere -- often via target-typed
        // `new(...)` inside the class's own static factory method, and
        // merely assigned here through a constructor parameter, never
        // reconstructed), and every real call site is in a *different*
        // file (`scope.Client.SomeRpcAsync(...)` in a `*Tests.cs` file, the
        // field declared in a `*ServiceScope.cs` fixture file). An earlier
        // version of this fix resolved that cross-file link incrementally,
        // during `extract()`, mirroring `ExtensionRegistry` -- which made
        // the result depend on directory sort order: dpb's
        // `Dpb.DataMgr.Tests/DataProduct` sorts before `.../Fixtures`, so
        // `TeamServiceTests.cs` (processed first) lost every edge, while
        // `SourcingIntegrationTests.cs` two directories over (whose
        // `Fixtures` happens to sort first) resolved fine -- identical
        // source shape, opposite outcome, purely from scan order. See
        // `GrpcClientFieldRegistry`'s doc.
        //
        // The fix moves cross-file resolution out of `extract()` entirely
        // into a one-time, whole-repo prescan
        // (`prescan_grpc_client_fields`, triggered from `resolve_imports`)
        // that reads every `.cs` file from disk directly rather than
        // relying on `extract()`'s own call order -- so this test
        // deliberately calls `extract()` on the *calling* file first, then
        // the *declaring* file, to prove the result no longer depends on
        // that order the way the incremental-registry version did.
        let dir = tempfile::tempdir().unwrap();
        let fixtures_dir = dir.path().join("Fixtures");
        let tests_dir = dir.path().join("Tests");
        std::fs::create_dir_all(&fixtures_dir).unwrap();
        std::fs::create_dir_all(&tests_dir).unwrap();

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
        std::fs::write(fixtures_dir.join("GreeterScope.cs"), fixture_source).unwrap();
        std::fs::write(tests_dir.join("GreeterTests.cs"), test_source).unwrap();

        let mut extractor = CSharpExtractor::new().unwrap();

        // Calling file FIRST.
        let mut test_file = extractor
            .extract(test_source, "tests/greeter_tests")
            .unwrap();
        assert!(
            !test_file
                .edges
                .iter()
                .any(|edge| edge.kind == proto::RPC_CALL_KIND),
            "must not resolve to a real RPC_CALL during extract() itself -- that immediate, \
             incrementally-built-registry resolution is exactly the order-dependent path this \
             test guards against, got {:?}",
            test_file.edges.iter().map(|e| &e.kind).collect::<Vec<_>>()
        );

        // Declaring file SECOND -- shouldn't matter either way, since
        // `resolve_imports`'s prescan reads it from disk, not from this
        // call.
        extractor
            .extract(fixture_source, "fixtures/greeter_scope")
            .unwrap();

        extractor.resolve_imports(
            dir.path(),
            "Tests/GreeterTests.cs",
            "tests/greeter_tests",
            &mut test_file.edges,
        );
        let calls = test_file
            .edges
            .iter()
            .filter(|edge| edge.kind == proto::RPC_CALL_KIND)
            .collect::<Vec<_>>();
        assert!(
            calls
                .iter()
                .any(|edge| edge.target_qualname.as_deref() == Some("/example.v1.greeter/sayhello")),
            "a gRPC client field declared in a different file must resolve after \
             resolve_imports, regardless of extract() call order, got {:?}",
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
    fn explicit_impls_of_members_get_distinct_identities() {
        let source = r#"
using N2;
namespace Acme {
public class C : IA<int>, IA<string>, N1.IB, N2.IB {
    void IA<int>.Run() {}
    void IA<string>.Run() {}
    void N1.IB.Go() {}
    void N2.IB.Go() {}
    int N1.IB.P { get; }
    event System.EventHandler N1.IB.Changed { add {} remove {} }
    public event System.EventHandler Other;
    public override void Base() {}
}
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let sym = |kind: &str| {
            file.symbols
                .iter()
                .filter(|s| s.kind == kind)
                .map(|s| s.qualname.as_str())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            sym("method"),
            [
                "Acme.C.IA<int>.Run",
                "Acme.C.IA<string>.Run",
                "Acme.C.N1.IB.Go",
                "Acme.C.N2.IB.Go",
                "Acme.C.Base"
            ]
        );
        assert_eq!(sym("property"), ["Acme.C.N1.IB.P"]);
        assert_eq!(sym("event"), ["Acme.C.N1.IB.Changed", "Acme.C.Other"]);
        assert_eq!(file.override_symbols.len(), 1);
        assert_eq!(file.override_symbols[0].0, "Acme.C.Base");
        // Base-list edges carry scope-ordered namespace guesses.
        let edge = file
            .edges
            .iter()
            .find(|e| e.kind == "IMPLEMENTS" && e.target_qualname.as_deref() == Some("IA"))
            .unwrap();
        // enclosing namespaces, the global namespace, then usings
        assert_eq!(edge.import_candidates, ["Acme.IA", "IA", "N2.IA"]);
        assert_eq!(edge.detail.as_deref(), Some("int"));
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

    #[test]
    fn strip_type_args_handles_nesting_and_qualification() {
        assert_eq!(strip_type_args("IRepo<Dictionary<K,V>>"), "IRepo");
        assert_eq!(strip_type_args("Ns.IRepo<T>"), "Ns.IRepo");
        assert_eq!(strip_type_args("Base<T>"), "Base");
        assert_eq!(strip_type_args("IPlain"), "IPlain");
    }

    /// An extension on a generic interface receiver (`this IRepo<T>`)
    /// must stay applicable when the call receiver is a derived interface:
    /// the generic-interface stripping used for dispatch must not leak into
    /// the extension-receiver path.
    #[test]
    fn generic_interface_extension_receiver_keeps_candidates() {
        let source = r#"
namespace Acme;
public static class Ext {
    public static int Total<T>(this IRepo<T> xs) => 0;
}
public class User {
    private readonly IMyList _list;
    public void Run() {
        _list.Total();
    }
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let call = file
            .edges
            .iter()
            .find(|e| {
                e.kind == "CALLS"
                    && e.evidence_snippet
                        .as_deref()
                        .is_some_and(|s| s.starts_with("_list.Total"))
            })
            .expect("CALLS edge");
        assert!(!call.import_candidates.is_empty(), "{call:?}");
    }

    /// Explicit generic type arguments (`_sql.QueryAsync<long>(...)`) used
    /// to leave `target_qualname` empty because the raw callee text carried
    /// the `<...>` list, which `is_simple_call_target` rejects. Each generic
    /// form must now match its non-generic twin exactly (target, receiver
    /// typing, extension candidates).
    #[test]
    fn generic_method_calls_get_same_target_as_non_generic() {
        let source = r#"
namespace Acme.Strategies;
public static class SqlHelperExt {
    public static Task<T> ProbeAsync<T>(this IngestSqlHelper h) => default;
}
public class IngestSqlHelper {
    public Task<T> QueryAsync<T>(SqlConnection c, string sql) => default;
}
public class ProductDeltaStrategy {
    private readonly IngestSqlHelper _sql;
    public async Task RunAsync(SqlConnection destConn, CancellationToken ct) {
        var (bid, err) = await _sql.QueryAsync<long?>(destConn, "select 1");
        var plain = await _sql.QueryAsync(destConn, "select 2");
        await _sql.ProbeAsync<int>();
        await _sql.ProbeAsync();
        var s = Factory.Create<Widget>();
        var b = Helper<int>(1);
        var t = this.Helper<int>(2);
        var m = await Mapper.Map<Dictionary<string, List<int>>>(plain);
        var l = new List<int>();
    }
    private int Helper<T>(T x) => 0;
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let call = |needle: &str| {
            file.edges
                .iter()
                .find(|e| {
                    e.kind == "CALLS"
                        && e.evidence_snippet
                            .as_deref()
                            .is_some_and(|s| s.starts_with(needle))
                })
                .unwrap_or_else(|| panic!("no CALLS edge for {needle}"))
        };
        let generic = call("_sql.QueryAsync<long?>");
        let plain = call("_sql.QueryAsync(destConn");
        // A generic *constructor* keeps its type args, so it can't be
        // bare-name bound to an unrelated `List` method.
        let ctor = call("new List<int>")
            .target_qualname
            .clone()
            .unwrap_or_default();
        assert!(!ctor.ends_with(".List") && ctor != "List", "{ctor}");
        assert_eq!(generic.target_qualname.as_deref(), Some("_sql.QueryAsync"));
        assert_eq!(generic.target_qualname, plain.target_qualname);
        assert_eq!(
            generic.receiver_type,
            ReceiverType::Known("IngestSqlHelper".to_string())
        );
        assert_eq!(generic.receiver_type, plain.receiver_type);
        let ext_generic = call("_sql.ProbeAsync<int>");
        let ext_plain = call("_sql.ProbeAsync()");
        assert!(!ext_plain.import_candidates.is_empty());
        assert_eq!(ext_generic.import_candidates, ext_plain.import_candidates);
        assert_eq!(ext_generic.target_qualname, ext_plain.target_qualname);
        assert_eq!(
            call("Factory.Create<Widget>").target_qualname.as_deref(),
            Some("Factory.Create")
        );
        assert_eq!(
            call("Helper<int>(1)").target_qualname.as_deref(),
            Some("Acme.Strategies.ProductDeltaStrategy.Helper")
        );
        assert_eq!(
            call("this.Helper<int>(2)").target_qualname.as_deref(),
            Some("Acme.Strategies.ProductDeltaStrategy.Helper")
        );
        assert_eq!(
            call("Mapper.Map<").target_qualname.as_deref(),
            Some("Mapper.Map")
        );
    }

    #[test]
    fn var_local_infers_receiver_type_from_same_file_return_type() {
        let source = r#"
namespace Acme;
public class Tests {
    private (Publisher, Bus) MakePublisher() => default;
    private static Store Open() => default;
    private async Task<Store> OpenAsync() => default;
    private Task<Store> Lazy() => default;
    private Store Dup(int a) => default;
    private Other Dup(string a) => default;
    private T Get<T>() => default;
    public async Task Run() {
        var (pub, bus) = MakePublisher();
        pub.PublishDeleted();
        bus.Flush();
        var s = Open();
        s.Write();
        var t = this.Open();
        t.Write2();
        var a = await OpenAsync();
        a.Write3();
        var l = Lazy();
        l.Write4();
        var d = Dup(1);
        d.Write5();
        var (x, _) = Unknown();
        x.Write6();
        var g = Get<Store>();
        g.Write7();
        var c = await OpenAsync().ConfigureAwait(false);
        c.Write8();
    }
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let recv = |needle: &str| {
            file.edges
                .iter()
                .find(|e| {
                    e.kind == "CALLS"
                        && e.evidence_snippet
                            .as_deref()
                            .is_some_and(|s| s.starts_with(needle))
                })
                .unwrap_or_else(|| panic!("no CALLS edge for {needle}"))
                .receiver_type
                .clone()
        };
        let known = |n: &str| ReceiverType::Known(n.to_string());
        assert_eq!(recv("pub.PublishDeleted"), known("Publisher"));
        assert_eq!(recv("bus.Flush"), known("Bus"));
        assert_eq!(recv("s.Write("), known("Store"));
        assert_eq!(recv("t.Write2"), known("Store"));
        assert_eq!(recv("a.Write3"), known("Store"));
        assert_eq!(recv("l.Write4"), ReceiverType::Unresolved);
        // Overloaded / generic same-file callees drop out of the same-file
        // table; the resolver sees the same ambiguity and stays untracked.
        let own = |m: &str| ReceiverType::deferred_return("Tests", m, false, false);
        assert_eq!(recv("d.Write5"), own("Dup"));
        assert_eq!(recv("x.Write6"), ReceiverType::Unresolved);
        assert_eq!(recv("g.Write7"), own("Get"));
        assert_eq!(recv("c.Write8"), known("Store"));
    }

    #[test]
    fn grpc_call_resolves_var_from_same_file_factory_return_type() {
        let source = r#"
using Example.V1;
public class Tests {
    private static Greeter.GreeterClient CreateGreeterClient(object factory) => default;
    public async Task Run(object factory) {
        var greeter = CreateGreeterClient(factory);
        await greeter.SayHelloAsync(new HelloRequest());
    }
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        assert!(
            file.edges.iter().any(|e| e.kind == proto::RPC_CALL_KIND
                && e.target_qualname.as_deref() == Some("/example.v1.greeter/sayhello")),
            "{:?}",
            file.edges
                .iter()
                .filter(|e| e.kind == proto::RPC_CALL_KIND)
                .map(|e| &e.target_qualname)
                .collect::<Vec<_>>()
        );
    }

    /// Receiver type of the first CALLS edge whose snippet starts with `needle`.
    trait WithNameOnly {
        fn with_name_only(self, name_only: bool) -> Self;
    }

    impl WithNameOnly for ReceiverType {
        fn with_name_only(self, name_only: bool) -> Self {
            match self {
                ReceiverType::Deferred(call) => {
                    ReceiverType::Deferred(DeferredReturn { name_only, ..call })
                }
                other => other,
            }
        }
    }

    fn on_call(inner: DeferredReturn, method: &str) -> ReceiverType {
        ReceiverType::Deferred(DeferredReturn::on_call(inner, method, false))
    }

    fn recv_of(file: &ExtractedFile, needle: &str) -> ReceiverType {
        file.edges
            .iter()
            .find(|e| {
                e.kind == "CALLS"
                    && e.evidence_snippet
                        .as_deref()
                        .is_some_and(|s| s.starts_with(needle))
            })
            .unwrap_or_else(|| panic!("no CALLS edge for {needle}"))
            .receiver_type
            .clone()
            // Whether the call's own target is name-only is asserted apart.
            .with_name_only(false)
    }

    #[test]
    fn foreach_deconstruction_binds_tuple_element_types() {
        let source = r#"
public class C {
    private List<(string, Bus)> _pairs;
    private Dictionary<string, Bus> _map;
    public void M(List<(string Name, Store S)> xs, (int, Bus)[] arr, IEnumerable<Store> plain) {
        foreach (var (k, v) in xs) { v.A(); }
        foreach (var (k2, b) in _pairs) { b.B(); }
        foreach (var (k3, m) in _map) { m.C(); }
        foreach (var (n, b2) in arr) { b2.D(); }
        foreach (var (p, q) in plain) { q.E(); }
        foreach (var (r, s) in Unknown()) { s.F(); }
        foreach (var (_, (t, u)) in xs) { u.G(); }
    }
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let known = |n: &str| ReceiverType::Known(n.to_string());
        assert_eq!(recv_of(&file, "v.A"), known("Store"));
        assert_eq!(recv_of(&file, "b.B"), known("Bus"));
        assert_eq!(recv_of(&file, "m.C"), known("Bus"));
        assert_eq!(recv_of(&file, "b2.D"), known("Bus"));
        assert_eq!(recv_of(&file, "q.E"), ReceiverType::Unresolved);
        assert_eq!(recv_of(&file, "s.F"), ReceiverType::Unresolved);
        assert_eq!(recv_of(&file, "u.G"), ReceiverType::Unresolved);
    }

    #[test]
    fn grpc_call_resolves_top_level_var_from_same_file_factory_return_type() {
        let source = r#"
using Example.V1;
var greeter = CreateGreeterClient(null);
await greeter.SayHelloAsync(new HelloRequest());
Greeter.GreeterClient CreateGreeterClient(object factory) => default;
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        assert!(
            file.edges.iter().any(|e| e.kind == proto::RPC_CALL_KIND
                && e.target_qualname.as_deref() == Some("/example.v1.greeter/sayhello")),
            "{:?}",
            file.edges
                .iter()
                .filter(|e| e.kind == proto::RPC_CALL_KIND)
                .map(|e| &e.target_qualname)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn cross_file_callee_defers_the_receiver_type() {
        let source = r#"
public class C {
    private Repo _repo;
    public async Task M(Repo repo, int n) {
        var a = repo.Open();
        a.A();
        var b = Repo.Create();
        b.B();
        var c = await _repo.OpenAsync().ConfigureAwait(false);
        c.C();
        var d = this._repo.Open();
        d.D();
        var e = n.Foo();
        e.E();
        var repo2 = Missing();
        var f = repo2.Open();
        f.F();
        var g = a.Open();
        g.G();
    }
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let deferred =
            |m: &str, aw: bool, st: bool| ReceiverType::deferred_return("Repo", m, aw, st);
        assert_eq!(recv_of(&file, "a.A"), deferred("Open", false, false));
        assert_eq!(recv_of(&file, "b.B"), deferred("Create", false, true));
        assert_eq!(recv_of(&file, "c.C"), deferred("OpenAsync", true, false));
        assert_eq!(recv_of(&file, "d.D"), deferred("Open", false, false));
        assert_eq!(recv_of(&file, "e.E"), ReceiverType::Unresolved);
        // A `var` bound from another deferred `var` nests its marker.
        let marker = |r: ReceiverType| match r {
            ReceiverType::Deferred(m) => m,
            other => panic!("not deferred: {other:?}"),
        };
        let missing = marker(ReceiverType::deferred_return("C", "Missing", false, false));
        assert_eq!(recv_of(&file, "f.F"), on_call(missing, "Open"));
        let open = marker(deferred("Open", false, false));
        assert_eq!(recv_of(&file, "g.G"), on_call(open, "Open"));
    }

    #[test]
    fn chained_this_and_base_calls_defer_the_receiver_type() {
        let source = r#"
public class D : B {
    private Repo _repo;
    public async Task M(Repo repo) {
        repo.Open().A();
        repo.Nested().Open().B();
        (await repo.OpenAsync()).C();
        var s = base.Inherited();
        s.D();
        var t = this.Local();
        t.E();
        var u = t.Next();
        var v = u.Next();
        v.F();
        _repo.Open().G();
        Local().H();
        Unknown.Open().I();
    }
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let marker = |r: ReceiverType| match r {
            ReceiverType::Deferred(m) => m,
            other => panic!("not deferred: {other:?}"),
        };
        let d =
            |ty: &str, m: &str, aw: bool, st: bool| ReceiverType::deferred_return(ty, m, aw, st);
        assert_eq!(
            recv_of(&file, "repo.Open().A"),
            d("Repo", "Open", false, false)
        );
        let nested = marker(d("Repo", "Nested", false, false));
        assert_eq!(
            recv_of(&file, "repo.Nested().Open().B"),
            on_call(nested, "Open")
        );
        assert_eq!(
            recv_of(&file, "(await repo.OpenAsync()).C"),
            d("Repo", "OpenAsync", true, false)
        );
        assert_eq!(recv_of(&file, "s.D"), d("B", "Inherited", false, false));
        assert_eq!(recv_of(&file, "t.E"), d("D", "Local", false, false));
        let t = marker(d("D", "Local", false, false));
        let u = marker(on_call(t, "Next"));
        assert_eq!(recv_of(&file, "v.F"), on_call(u, "Next"));
        assert_eq!(
            recv_of(&file, "_repo.Open().G"),
            d("Repo", "Open", false, false)
        );
        assert_eq!(recv_of(&file, "Local().H"), d("D", "Local", false, false));
        assert_eq!(
            recv_of(&file, "Unknown.Open().I"),
            d("Unknown", "Open", false, true)
        );
        // A chained call's target is its bare method name, never a placeholder.
        let chained = file
            .edges
            .iter()
            .find(|e| {
                e.evidence_snippet
                    .as_deref()
                    .is_some_and(|s| s.starts_with("repo.Open().A"))
            })
            .unwrap();
        assert_eq!(chained.target_qualname.as_deref(), Some("A"));
        assert!(matches!(&chained.receiver_type, ReceiverType::Deferred(c) if c.name_only));
    }

    #[test]
    fn signatures_keep_generic_return_types_and_static_is_recorded() {
        let source = r#"
public class Box<T> {
    public T Get() => default;
    public static Store Plain() => null;
    public U Map<U>(int x) => default;
    public static int Over(int a) => 0;
    public int Over(string a) => 0;
    public static int Twice(int a) => 0;
    public static int Twice(string a) => 0;
}
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        let sig = |name: &str| {
            file.symbols
                .iter()
                .find(|s| s.name == name)
                .unwrap()
                .signature
                .clone()
        };
        assert_eq!(sig("Get").as_deref(), Some("() -> T"));
        assert_eq!(sig("Map").as_deref(), Some("(int x) -> U"));
        assert_eq!(sig("Plain").as_deref(), Some("() -> Store"));
        let mut statics = file.static_member_qualnames.clone();
        statics.sort();
        assert_eq!(statics, vec!["module.Box.Plain", "module.Box.Twice"]);
        assert_eq!(
            receiver_from_signature("() -> Task<Store>", true).as_deref(),
            Some("Store")
        );
        assert_eq!(receiver_from_signature("() -> Task<Store>", false), None);
        assert_eq!(receiver_from_signature("() -> int", false), None);
        assert_eq!(receiver_from_signature("()", false), None);
    }

    #[test]
    fn top_level_statements_get_local_types() {
        let source = r#"
var store = new Store();
store.A();
var made = Repo.Create();
made.B();
public class C { public void M() { store.Z(); } }
"#;
        let mut extractor = CSharpExtractor::new().unwrap();
        let file = extractor.extract(source, "module").unwrap();
        assert_eq!(
            recv_of(&file, "store.A"),
            ReceiverType::Known("Store".into())
        );
        assert_eq!(
            recv_of(&file, "made.B"),
            ReceiverType::deferred_return("Repo", "Create", false, true)
        );
        // Top-level locals aren't in scope inside a type.
        assert_eq!(recv_of(&file, "store.Z"), ReceiverType::NotTracked);
    }
}
