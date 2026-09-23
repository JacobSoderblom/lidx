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
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tree_sitter::{Node, Parser};

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
    module: String,
    class_stack: Vec<String>,
    fn_depth: usize,
    current_scope: String,
    route_prefix: Option<String>,
    router_aliases: Vec<String>,
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
}

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
    let mut resolved = Vec::new();
    for edge in edges.iter() {
        if edge.kind != "IMPORTS" {
            continue;
        }
        let target = match edge.target_qualname.as_deref() {
            Some(value) => value.trim(),
            None => continue,
        };
        if target.is_empty() {
            continue;
        }
        let dst_rel = match resolve_import_path(repo_root, file_rel_path, target) {
            Some(value) => value,
            None => continue,
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
                    "confidence": 1.0,
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

fn resolve_import_path(repo_root: &Path, file_rel_path: &str, target: &str) -> Option<String> {
    let target = target.split(['?', '#']).next().unwrap_or(target).trim();
    if target.is_empty() {
        return None;
    }
    let is_relative =
        target.starts_with("./") || target.starts_with("../") || target.starts_with('/');
    if !is_relative {
        // Not a relative specifier: it's either a genuine third-party import
        // (e.g. `next/navigation`) or an alias remapped through the owning
        // tsconfig.json's `compilerOptions.paths` (e.g. `@/lib/foo`). Only
        // the latter ever resolves, and only when a concrete path-mapping
        // entry backs it *and* the mapped location is a real file — no
        // fuzzy fallback, so an unmapped bare specifier stays unresolved
        // exactly as before.
        return resolve_tsconfig_alias(repo_root, file_rel_path, target);
    }
    let base_dir = Path::new(file_rel_path)
        .parent()
        .unwrap_or_else(|| Path::new(""));
    let rel = if target.starts_with('/') {
        PathBuf::from(target.trim_start_matches('/'))
    } else {
        let mut rel = PathBuf::from(base_dir);
        rel.push(target);
        rel
    };
    probe_module_candidates(repo_root, &rel)
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
        return None;
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
/// Returns `None` (never a guess) unless a `paths` entry syntactically
/// matches the specifier *and* the mapped location, run back through the
/// same extension/index probing relative imports use, is a real file.
fn resolve_tsconfig_alias(repo_root: &Path, file_rel_path: &str, target: &str) -> Option<String> {
    let config_dir = find_owning_tsconfig_dir(repo_root, file_rel_path)?;
    let aliases = load_tsconfig_aliases(repo_root, &config_dir)?;
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
            if let Some(resolved) = probe_module_candidates(repo_root, &rel) {
                return Some(resolved);
            }
        }
    }
    None
}

/// Walks from `file_rel_path`'s directory up toward `repo_root`, returning
/// the directory (relative to `repo_root`) of the nearest ancestor
/// `tsconfig.json`, if any. This is what makes alias resolution per-project
/// rather than global: a file under `node/dpb-app/` finds
/// `node/dpb-app/tsconfig.json` before it ever sees
/// `node/datacatalog-ui/tsconfig.json`, even though both define `@/*`.
fn find_owning_tsconfig_dir(repo_root: &Path, file_rel_path: &str) -> Option<PathBuf> {
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

// ponytail: a tsconfig that `extends` another one (relative path or, like
// `@docusaurus/tsconfig`, a package) is read for its own `compilerOptions`
// only — an extended `paths`/`baseUrl` isn't inherited. Ceiling: an alias
// defined solely in a base config the project extends resolves nothing
// here. Neither `node/datacatalog-ui/tsconfig.json` nor
// `node/dpb-app/tsconfig.json` (the two configs this fix targets) extend
// anything, so this doesn't affect either. Follow the (relative-path-only)
// `extends` chain here if a project that needs it is reported.
fn load_tsconfig_aliases(repo_root: &Path, config_dir: &Path) -> Option<TsconfigAliases> {
    let raw = util::read_to_string(&repo_root.join(config_dir).join("tsconfig.json")).ok()?;
    let cleaned = strip_jsonc(&raw);
    let value: serde_json::Value = serde_json::from_str(&cleaned).ok()?;
    let compiler_options = value.get("compilerOptions")?;
    let paths = compiler_options.get("paths")?.as_object()?;
    if paths.is_empty() {
        return None;
    }
    let base_url = compiler_options
        .get("baseUrl")
        .and_then(|v| v.as_str())
        .unwrap_or(".");
    let base_dir = config_dir.join(base_url);

    let mut entries: Vec<(String, Vec<String>)> = Vec::new();
    for (pattern, targets_value) in paths {
        let Some(targets_array) = targets_value.as_array() else {
            continue;
        };
        let targets: Vec<String> = targets_array
            .iter()
            .filter_map(|t| t.as_str().map(|s| s.to_string()))
            .collect();
        if targets.is_empty() {
            continue;
        }
        entries.push((pattern.clone(), targets));
    }
    if entries.is_empty() {
        return None;
    }
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
    if let Some(edge) = next_api_route_edge(module_name) {
        output.edges.push(edge);
    }
    let grpc_clients = collect_grpc_clients(root, source);
    let ctx = Context {
        module: module_name.to_string(),
        class_stack: Vec::new(),
        fn_depth: 0,
        current_scope: module_name.to_string(),
        route_prefix: None,
        router_aliases: Vec::new(),
        grpc_clients,
        local_types: Rc::new(infer_module_level_types(root, source)),
        class_attr_types: Rc::new(HashMap::new()),
        fn_owner: None,
    };
    walk_node(root, &ctx, source, &mut output);
    Ok(output)
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
    if current.kind() != "new_expression" && current.kind() != "call_expression" {
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
        // module scope: a handler const inside a component/function body
        // stays attributed to that component, like it does for a
        // `function` declaration. Destructuring (`const {a} = f()`) has no
        // single owner, so it's left alone.
        if ctx.current_scope == ctx.module
            && ctx.class_stack.is_empty()
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
        }
        _ => {}
    }

    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        walk_node(child, ctx, source, output);
    }
}

fn handle_class(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
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
    for target in collect_clause_targets_from(node, "extends_clause", source) {
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
        let name = node_text(child, source);
        if !name.is_empty() {
            targets.push(name);
        }
    }
    targets
}

fn walk_class_body(node: Node<'_>, ctx: &Context, source: &str, output: &mut ExtractedFile) {
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() == "method_definition" {
            handle_method(child, ctx, source, output);
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
        ..Default::default()
    });
    register_handled
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
    if is_http_client_call(node, source) {
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
    let raw_topic = args
        .first()
        .and_then(|arg| extract_string_literal(*arg, source))
        .or_else(|| args.first().map(|arg| node_text(*arg, source)))?;
    let normalized = channel::normalize_channel_name(&raw_topic)?;
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
    grpc_service_from_path(receiver)
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
        .and_then(|arg| extract_string_literal(*arg, source))?;
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
    if is_axios_identifier(target_node, source) {
        let config = args.first()?;
        let raw_path = object_property_string(config, "url", source)?;
        let normalized = http::normalize_path(&raw_path)?;
        let method = object_property_string(config, "method", source)
            .and_then(|raw| http::normalize_method(&raw))
            .unwrap_or_else(|| "GET".to_string());
        let detail = http::build_call_detail(&method, &normalized, &raw_path, "axios");
        return Some(EdgeInput {
            kind: http::HTTP_CALL_KIND.to_string(),
            source_qualname: Some(ctx.current_scope.clone()),
            target_qualname: Some(normalized),
            detail: Some(detail),
            evidence_snippet: None,
            evidence_start_line: Some(span(node).0),
            evidence_end_line: Some(span(node).2),
            ..Default::default()
        });
    }
    let (receiver, method_name) = member_receiver_and_method(target_node, source)?;
    if receiver != "axios" {
        return None;
    }
    if !HTTP_METHOD_NAMES.contains(&method_name.as_str()) {
        return None;
    }
    let raw_path = args
        .first()
        .and_then(|arg| extract_string_literal(*arg, source))?;
    let normalized = http::normalize_path(&raw_path)?;
    let method = http::normalize_method(&method_name)?;
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
}

fn is_http_client_call(node: Node<'_>, source: &str) -> bool {
    let Some(target_node) = call_target_node(node) else {
        return false;
    };
    is_fetch_callee(target_node, source) || is_axios_callee(target_node, source)
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

fn jsx_route_edge(node: Node<'_>, ctx: &Context, source: &str) -> Option<EdgeInput> {
    let opening = match node.kind() {
        "jsx_element" => node.child_by_field_name("opening_element")?,
        "jsx_self_closing_element" => node,
        _ => return None,
    };
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

fn next_page_route_edge(module_name: &str) -> Option<EdgeInput> {
    let raw = next_route_from_module(module_name, false)?;
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

fn next_api_route_edge(module_name: &str) -> Option<EdgeInput> {
    let raw = next_route_from_module(module_name, true)?;
    let normalized = http::normalize_path(&raw)?;
    let detail = http::build_route_detail(http::HTTP_ANY, &normalized, &raw, "nextjs");
    Some(EdgeInput {
        kind: http::HTTP_ROUTE_KIND.to_string(),
        source_qualname: Some(module_name.to_string()),
        target_qualname: Some(normalized),
        detail: Some(detail),
        evidence_snippet: None,
        evidence_start_line: None,
        evidence_end_line: None,
        ..Default::default()
    })
}

fn next_route_from_module(module_name: &str, api_only: bool) -> Option<String> {
    let parts: Vec<&str> = module_name.split('/').collect();
    if parts.is_empty() {
        return None;
    }
    let is_pages = parts.first() == Some(&"pages");
    let is_app = parts.first() == Some(&"app");
    if !is_pages && !is_app {
        return None;
    }
    let mut segments = parts[1..].to_vec();
    if api_only {
        if segments.first() != Some(&"api") {
            return None;
        }
        segments.remove(0);
        if let Some(last) = segments.last()
            && *last == "route"
        {
            segments.pop();
        }
    } else {
        if segments.first() == Some(&"api") {
            return None;
        }
        if is_app {
            if let Some(last) = segments.last()
                && *last != "page"
            {
                return None;
            }
            segments.pop();
        }
    }
    let mut out = String::from("/");
    let mut first = true;
    for seg in segments {
        if seg.is_empty() || seg.starts_with('(') {
            continue;
        }
        if seg == "index" {
            continue;
        }
        if !first {
            out.push('/');
        }
        first = false;
        let normalized = seg
            .trim_start_matches('[')
            .trim_end_matches(']')
            .trim_start_matches("...");
        if seg.starts_with('[') && seg.ends_with(']') {
            out.push(':');
            out.push_str(normalized);
        } else {
            out.push_str(seg);
        }
    }
    if out.is_empty() {
        out.push('/');
    }
    Some(out)
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
    let name = node_text(name_node, source);
    if name.is_empty() {
        return;
    }
    let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
    let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(node);
    let signature = extract_signature(node, source);
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
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(ctx.module.clone()),
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
    if let Some(body) = node.child_by_field_name("body") {
        let mut next_ctx = ctx.clone();
        next_ctx.fn_depth += 1;
        next_ctx.current_scope = build_qualname(&ctx.module, &ctx.class_stack, &name);
        next_ctx.local_types = Rc::new(infer_local_types(node, source));
        walk_node(body, &next_ctx, source, output);
    }
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
    });
    output.edges.push(EdgeInput {
        kind: "CONTAINS".to_string(),
        source_qualname: Some(ctx.module.clone()),
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
    if !ctx.class_stack.is_empty() {
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
        let name = node_text(name_node, source);
        if name.is_empty() {
            continue;
        }
        let qualname = build_qualname(&ctx.module, &ctx.class_stack, &name);
        let (start_line, start_col, end_line, end_col, start_byte, end_byte) = span(child);
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

/// Infer the receiver type of a call's callee expression (`function_node`),
/// mirroring `python::infer_receiver_type` with `this` standing in for
/// `self`/`cls`. Only gates resolution; never changes `target_qualname`
/// (see `resolve_call_target`, which stays text-based and keeps the
/// receiver's literal text for evidence).
///
/// Rules, in order:
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
    use crate::indexer::extract::LanguageExtractor;
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
}
