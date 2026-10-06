//! Declared Python types, as a per-file payload.
//!
//! The extractor (`python.rs`) builds a [`PyFileDecls`] for every Python file
//! from tree-sitter nodes: imports, classes with their bases and attributes,
//! functions with their annotated signatures, and module-level variables.
//! The payload is persisted per file in the `py_decls` table (migration 28)
//! and loaded whole into a type table by the call evaluator. [`lower_type`]
//! is the only place annotation syntax is interpreted.
//!
//! Expression-valued attribute sources (`self.x = Foo()`, `x = Foo()` in a
//! class body) are lowered with `python_lower` into [`PyExpr`] values, so the
//! evaluator never re-reads source text.

use crate::indexer::python::{RawImport, join_from_import_target, raw_import};
use crate::indexer::python::{absolutize_module, base_package_parts, definition_span};
use crate::indexer::python_expr::{PyExpr, Why};
use crate::indexer::python_lower::{Scope, lower_expr};
use crate::indexer::tree_helpers::node_text;
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::HashMap;
use tree_sitter::{Node, Parser};

fn is_false(b: &bool) -> bool {
    !*b
}

fn is_zero(n: &u8) -> bool {
    *n == 0
}

/// A type annotation as written, normalized: `Optional[X]`, `X | None` and
/// `Union[X, None]` are `X`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub enum PyTypeRef {
    /// `pkg.Foo[int]`: a dotted path and type arguments.
    #[serde(rename = "N")]
    Name {
        #[serde(rename = "p")]
        path: Vec<String>,
        #[serde(rename = "a", default, skip_serializing_if = "Vec::is_empty")]
        args: Vec<PyTypeRef>,
    },
    /// Two or more members left after removing `None`.
    #[serde(rename = "U")]
    Union(Vec<PyTypeRef>),
    #[serde(rename = "O")]
    None,
    /// Anything not understood: calls, literals, parse gaps.
    #[serde(rename = "?")]
    Unknown,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PyImport {
    /// The name the import binds in the module's scope.
    #[serde(rename = "b")]
    pub bound: String,
    /// Absolute dotted target: a module, or (for `from` imports) a member.
    #[serde(rename = "t")]
    pub target: String,
    /// `from a.b import C`: `target` is `a.b.C`, which may be a member of
    /// module `a.b` rather than a module.
    #[serde(rename = "m", default, skip_serializing_if = "is_false")]
    pub member: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Default)]
pub struct PyFileDecls {
    /// Module name, equal to the one the file's symbols use.
    #[serde(rename = "m")]
    pub module: String,
    /// The file is a package `__init__.py`.
    #[serde(rename = "p", default, skip_serializing_if = "is_false")]
    pub is_package: bool,
    #[serde(rename = "i", default, skip_serializing_if = "Vec::is_empty")]
    pub imports: Vec<PyImport>,
    /// `from m import *`, absolutized.
    #[serde(rename = "si", default, skip_serializing_if = "Vec::is_empty")]
    pub star_imports: Vec<String>,
    /// `__all__` when it is a literal list/tuple of strings.
    #[serde(rename = "al", default, skip_serializing_if = "Option::is_none")]
    pub all: Option<Vec<String>>,
    #[serde(rename = "c", default, skip_serializing_if = "Vec::is_empty")]
    pub classes: Vec<PyClassDecl>,
    #[serde(rename = "f", default, skip_serializing_if = "Vec::is_empty")]
    pub functions: Vec<PyFuncDecl>,
    #[serde(rename = "v", default, skip_serializing_if = "Vec::is_empty")]
    pub vars: Vec<PyVarDecl>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PyClassDecl {
    #[serde(rename = "q")]
    pub qualname: String,
    #[serde(rename = "n")]
    pub name: String,
    #[serde(rename = "b", default, skip_serializing_if = "Vec::is_empty")]
    pub bases: Vec<PyTypeRef>,
    #[serde(rename = "a", default, skip_serializing_if = "Vec::is_empty")]
    pub attrs: Vec<PyAttrDecl>,
    #[serde(rename = "l")]
    pub start_line: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PyAttrDecl {
    #[serde(rename = "n")]
    pub name: String,
    /// In source order. The evaluator takes every source that evaluates to a
    /// type and requires them to agree.
    #[serde(rename = "s")]
    pub sources: Vec<PyAttrSource>,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum PyAttrSource {
    /// `x: T` / `x: T = v` in the class body, or `self.x: T = ...`.
    #[serde(rename = "d")]
    Declared(PyTypeRef),
    /// `x = v` in the class body, or `self.x = v` in any method (lowered in
    /// that method's scope).
    #[serde(rename = "v")]
    Value(PyExpr),
    /// A `@property` / `cached_property` getter's return annotation.
    #[serde(rename = "p")]
    Property(PyTypeRef),
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum PyFuncKind {
    Function,
    Method,
    StaticMethod,
    ClassMethod,
    Property,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PyParam {
    #[serde(rename = "n")]
    pub name: String,
    #[serde(rename = "t", default, skip_serializing_if = "Option::is_none")]
    pub ty: Option<PyTypeRef>,
    /// 1 for `*args`, 2 for `**kwargs`.
    #[serde(rename = "s", default, skip_serializing_if = "is_zero")]
    pub star: u8,
    #[serde(rename = "d", default, skip_serializing_if = "is_false")]
    pub has_default: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PyFuncDecl {
    #[serde(rename = "q")]
    pub qualname: String,
    #[serde(rename = "n")]
    pub name: String,
    /// Index of the owning class in `PyFileDecls::classes`.
    #[serde(rename = "o", default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<u32>,
    #[serde(rename = "k")]
    pub kind: PyFuncKind,
    /// All parameters in order, `self` / `cls` included (with its annotation
    /// when written, e.g. `def __enter__(self: T) -> T`).
    #[serde(rename = "p", default, skip_serializing_if = "Vec::is_empty")]
    pub params: Vec<PyParam>,
    #[serde(rename = "r", default, skip_serializing_if = "Option::is_none")]
    pub returns: Option<PyTypeRef>,
    #[serde(rename = "as", default, skip_serializing_if = "is_false")]
    pub is_async: bool,
    /// An `@overload` stub; the evaluator binds to the implementation.
    #[serde(rename = "ov", default, skip_serializing_if = "is_false")]
    pub is_overload: bool,
    /// A method with no return annotation whose every `return` statement is
    /// exactly `return self` (and has at least one): the evaluator types its
    /// call like `-> Self`.
    #[serde(rename = "rs", default, skip_serializing_if = "is_false")]
    pub returns_self: bool,
    #[serde(rename = "l")]
    pub start_line: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PyVarDecl {
    #[serde(rename = "n")]
    pub name: String,
    #[serde(rename = "t", default, skip_serializing_if = "Option::is_none")]
    pub ty: Option<PyTypeRef>,
    /// Present only when the name is bound exactly once in the module.
    #[serde(rename = "v", default, skip_serializing_if = "Option::is_none")]
    pub value: Option<PyExpr>,
}

/// One live file's declarations, as `Db::py_decls` loads them.
#[derive(Clone, Debug)]
pub struct PyLoadedFile {
    pub file_id: i64,
    pub path: String,
    pub decls: PyFileDecls,
}

impl PyFileDecls {
    /// Compact JSON payload stored in `py_decls.payload`.
    pub fn to_payload(&self) -> String {
        // Plain structs of strings/vecs/maps with string keys: serde_json cannot fail on them.
        serde_json::to_string(self).expect("PyFileDecls serializes")
    }

    pub fn from_payload(payload: &str) -> serde_json::Result<Self> {
        serde_json::from_str(payload)
    }

    /// blake3 of the payload: the "declarations changed" gate.
    pub fn hash(&self) -> String {
        payload_hash(&self.to_payload())
    }
}

/// blake3 hex digest of a stored `py_decls` payload.
pub fn payload_hash(payload: &str) -> String {
    blake3::hash(payload.as_bytes()).to_hex().to_string()
}

// ---------------------------------------------------------------------------
// Annotation lowering
// ---------------------------------------------------------------------------

const MAX_TYPE_DEPTH: usize = 16;

/// Lower an annotation node (a `type` node or any expression) to a
/// [`PyTypeRef`].
pub fn lower_type(node: Node<'_>, source: &str) -> PyTypeRef {
    lower_ty(node, source, 0)
}

fn lower_ty(node: Node<'_>, source: &str, depth: usize) -> PyTypeRef {
    if depth > MAX_TYPE_DEPTH {
        return PyTypeRef::Unknown;
    }
    match node.kind() {
        "type" | "parenthesized_expression" => {
            node.named_child(0).map_or(PyTypeRef::Unknown, |inner| {
                lower_ty(inner, source, depth + 1)
            })
        }
        "identifier" => PyTypeRef::Name {
            path: vec![node_text(node, source)],
            args: Vec::new(),
        },
        "attribute" => match dotted_path(node, source) {
            Some(path) => PyTypeRef::Name {
                path,
                args: Vec::new(),
            },
            None => PyTypeRef::Unknown,
        },
        "none" => PyTypeRef::None,
        "string" => forward_ref(node, source, depth),
        "generic_type" => {
            let Some(head) = node.named_child(0).and_then(|h| dotted_path(h, source)) else {
                return PyTypeRef::Unknown;
            };
            let mut args = Vec::new();
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if child.kind() == "type_parameter" {
                    let mut inner = child.walk();
                    for arg in child.named_children(&mut inner) {
                        args.push(lower_ty(arg, source, depth + 1));
                    }
                }
            }
            named(head, args)
        }
        "subscript" => {
            let Some(head) = node
                .child_by_field_name("value")
                .and_then(|h| dotted_path(h, source))
            else {
                return PyTypeRef::Unknown;
            };
            let mut args = Vec::new();
            let mut cursor = node.walk();
            for arg in node.children_by_field_name("subscript", &mut cursor) {
                if matches!(arg.kind(), "expression_list" | "tuple") {
                    let mut inner = arg.walk();
                    for a in arg.named_children(&mut inner) {
                        args.push(lower_ty(a, source, depth + 1));
                    }
                } else {
                    args.push(lower_ty(arg, source, depth + 1));
                }
            }
            named(head, args)
        }
        "binary_operator" | "union_type" => {
            let mut members = Vec::new();
            if collect_union(node, source, depth, &mut members) {
                normalize_union(members)
            } else {
                PyTypeRef::Unknown
            }
        }
        _ => PyTypeRef::Unknown,
    }
}

/// Flatten `A | B | C`; false when an operator other than `|` shows up.
fn collect_union(node: Node<'_>, source: &str, depth: usize, out: &mut Vec<PyTypeRef>) -> bool {
    match node.kind() {
        "binary_operator" => {
            let is_pipe = node
                .child_by_field_name("operator")
                .is_some_and(|op| node_text(op, source) == "|");
            match (
                is_pipe,
                node.child_by_field_name("left"),
                node.child_by_field_name("right"),
            ) {
                (true, Some(l), Some(r)) => {
                    collect_union(l, source, depth + 1, out)
                        && collect_union(r, source, depth + 1, out)
                }
                _ => false,
            }
        }
        "union_type" => {
            let mut cursor = node.walk();
            node.named_children(&mut cursor)
                .all(|c| collect_union(c, source, depth + 1, out))
        }
        _ => {
            out.push(lower_ty(node, source, depth + 1));
            true
        }
    }
}

/// `a.b.c` as segments; `None` for anything else.
fn dotted_path(node: Node<'_>, source: &str) -> Option<Vec<String>> {
    match node.kind() {
        "identifier" => Some(vec![node_text(node, source)]),
        "attribute" => {
            let mut path = dotted_path(node.child_by_field_name("object")?, source)?;
            path.push(node_text(node.child_by_field_name("attribute")?, source));
            Some(path)
        }
        _ => None,
    }
}

/// A subscripted name, with the typing wrappers applied.
fn named(path: Vec<String>, args: Vec<PyTypeRef>) -> PyTypeRef {
    let last = path.last().map(String::as_str).unwrap_or("");
    match last {
        "Optional" if args.len() == 1 => normalize_union(vec![args[0].clone(), PyTypeRef::None]),
        "Union" => normalize_union(args),
        "Annotated" | "ClassVar" | "Final" if !args.is_empty() => args[0].clone(),
        // The arguments are values, not types.
        "Literal" => PyTypeRef::Name {
            path,
            args: Vec::new(),
        },
        _ => PyTypeRef::Name { path, args },
    }
}

/// Flatten nested unions, drop `None`, drop duplicates: no members left is
/// `None`, one is that member.
fn normalize_union(members: Vec<PyTypeRef>) -> PyTypeRef {
    let mut flat: Vec<PyTypeRef> = Vec::new();
    fn push(flat: &mut Vec<PyTypeRef>, t: PyTypeRef) {
        match t {
            PyTypeRef::None => {}
            PyTypeRef::Union(inner) => inner.into_iter().for_each(|i| push(flat, i)),
            other => {
                if !flat.contains(&other) {
                    flat.push(other);
                }
            }
        }
    }
    for m in members {
        push(&mut flat, m);
    }
    match flat.len() {
        0 => PyTypeRef::None,
        1 => flat.remove(0),
        _ => PyTypeRef::Union(flat),
    }
}

thread_local! {
    static FORWARD_PARSER: RefCell<Option<Parser>> = const { RefCell::new(None) };
}

/// `"Foo | None"`: parse the string's text as an expression.
fn forward_ref(node: Node<'_>, source: &str, depth: usize) -> PyTypeRef {
    let mut text = String::new();
    let mut cursor = node.walk();
    for child in node.children(&mut cursor) {
        match child.kind() {
            "string_start" => {
                let prefix = node_text(child, source).to_ascii_lowercase();
                if prefix.contains('b') || prefix.contains('f') {
                    return PyTypeRef::Unknown;
                }
            }
            "string_content" => text.push_str(&node_text(child, source)),
            "string_end" => {}
            _ => return PyTypeRef::Unknown,
        }
    }
    let text = text.trim();
    if text.is_empty() {
        return PyTypeRef::Unknown;
    }
    FORWARD_PARSER.with(|slot| {
        let mut slot = slot.borrow_mut();
        if slot.is_none() {
            let mut parser = Parser::new();
            if parser
                .set_language(&tree_sitter_python::LANGUAGE.into())
                .is_err()
            {
                return PyTypeRef::Unknown;
            }
            *slot = Some(parser);
        }
        let Some(tree) = slot.as_mut().and_then(|p| p.parse(text, None)) else {
            return PyTypeRef::Unknown;
        };
        let root = tree.root_node();
        if root.has_error() || root.named_child_count() != 1 {
            return PyTypeRef::Unknown;
        }
        let Some(stmt) = root
            .named_child(0)
            .filter(|s| s.kind() == "expression_statement")
        else {
            return PyTypeRef::Unknown;
        };
        match stmt.named_child(0) {
            Some(expr) => lower_ty(expr, text, depth + 1),
            None => PyTypeRef::Unknown,
        }
    })
}

// ---------------------------------------------------------------------------
// Parameters
// ---------------------------------------------------------------------------

/// Every parameter of a `parameters` node, in order.
pub(crate) fn param_infos(params: Node<'_>, source: &str) -> Vec<PyParam> {
    let mut out = Vec::new();
    let mut cursor = params.walk();
    for child in params.named_children(&mut cursor) {
        let mut star = 0u8;
        let mut has_default = false;
        let mut ty = None;
        let name_node = match child.kind() {
            "identifier" => Some(child),
            "typed_parameter" => {
                ty = child.child_by_field_name("type");
                child.named_child(0)
            }
            "default_parameter" => {
                has_default = true;
                child.child_by_field_name("name")
            }
            "typed_default_parameter" => {
                has_default = true;
                ty = child.child_by_field_name("type");
                child.child_by_field_name("name")
            }
            "list_splat_pattern" | "dictionary_splat_pattern" => Some(child),
            _ => None,
        };
        let Some(mut name_node) = name_node else {
            continue;
        };
        match name_node.kind() {
            "list_splat_pattern" => star = 1,
            "dictionary_splat_pattern" => star = 2,
            _ => {}
        }
        if star > 0 {
            match name_node.named_child(0) {
                Some(inner) => name_node = inner,
                None => continue,
            }
        }
        if name_node.kind() != "identifier" {
            continue;
        }
        out.push(PyParam {
            name: node_text(name_node, source),
            ty: ty.map(|t| lower_type(t, source)),
            star,
            has_default,
        });
    }
    out
}

// ---------------------------------------------------------------------------
// File declarations
// ---------------------------------------------------------------------------

/// Build a file's declarations. `module` is the module name the file's
/// symbols use; `rel_path` (when known) tells a package `__init__.py` from a
/// plain module, which decides what a relative import is relative to.
pub fn build_file_decls(
    root: Node<'_>,
    source: &str,
    module: &str,
    rel_path: Option<&str>,
) -> PyFileDecls {
    let rel = rel_path.unwrap_or("");
    let is_package = std::path::Path::new(rel)
        .file_name()
        .and_then(|s| s.to_str())
        == Some("__init__.py");
    let mut builder = Builder {
        source,
        module: module.to_string(),
        base_package: base_package_parts(rel, module),
        decls: PyFileDecls {
            module: module.to_string(),
            is_package,
            ..Default::default()
        },
        vars: Vec::new(),
        var_index: HashMap::new(),
    };
    let mut class_stack = Vec::new();
    builder.walk(root, &mut class_stack, None);
    builder.finish()
}

/// Bindings of one module-level name.
struct VarAcc {
    name: String,
    /// Assigned or annotated (a variable, not just a def/class/import).
    is_var: bool,
    count: usize,
    ty: Option<PyTypeRef>,
    value: Option<PyExpr>,
}

struct Builder<'a> {
    source: &'a str,
    module: String,
    base_package: Vec<String>,
    decls: PyFileDecls,
    vars: Vec<VarAcc>,
    var_index: HashMap<String, usize>,
}

impl<'a> Builder<'a> {
    fn finish(mut self) -> PyFileDecls {
        for acc in self.vars {
            if !acc.is_var {
                continue;
            }
            self.decls.vars.push(PyVarDecl {
                name: acc.name,
                ty: acc.ty,
                value: if acc.count == 1 { acc.value } else { None },
            });
        }
        self.decls
    }

    fn var(&mut self, name: &str) -> &mut VarAcc {
        let idx = match self.var_index.get(name) {
            Some(i) => *i,
            None => {
                self.vars.push(VarAcc {
                    name: name.to_string(),
                    is_var: false,
                    count: 0,
                    ty: None,
                    value: None,
                });
                self.var_index.insert(name.to_string(), self.vars.len() - 1);
                self.vars.len() - 1
            }
        };
        &mut self.vars[idx]
    }

    /// A module-level name bound by something that is not a variable
    /// assignment (def, class, import): counts as a (re)binding.
    fn note_binding(&mut self, name: &str) {
        self.var(name).count += 1;
    }

    fn walk<'t>(&mut self, node: Node<'t>, class_stack: &mut Vec<String>, owner: Option<usize>) {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            match child.kind() {
                "function_definition" => self.function(child, &[], class_stack, owner),
                "class_definition" => self.class(child, class_stack),
                "decorated_definition" => {
                    let mut decorators = Vec::new();
                    let mut definition = None;
                    let mut dc = child.walk();
                    for part in child.named_children(&mut dc) {
                        match part.kind() {
                            "decorator" => decorators.push(part),
                            "function_definition" | "class_definition" => definition = Some(part),
                            _ => {}
                        }
                    }
                    match definition {
                        Some(def) if def.kind() == "class_definition" => {
                            self.class(def, class_stack)
                        }
                        Some(def) => self.function(def, &decorators, class_stack, owner),
                        None => {}
                    }
                }
                "import_statement" | "import_from_statement" => {
                    if class_stack.is_empty() {
                        self.import(child);
                    }
                }
                "expression_statement" => self.statement(child, class_stack, owner),
                "for_statement" => {
                    if class_stack.is_empty()
                        && let Some(left) = child.child_by_field_name("left")
                    {
                        self.bind_targets(left);
                    }
                    self.walk(child, class_stack, owner);
                }
                // Anything that is not an expression can hold definitions
                // (if / try / with / blocks).
                _ => self.walk(child, class_stack, owner),
            }
        }
    }

    fn bind_targets(&mut self, node: Node<'_>) {
        match node.kind() {
            "identifier" => {
                let name = node_text(node, self.source);
                let acc = self.var(&name);
                acc.is_var = true;
                acc.count += 1;
            }
            "attribute" | "subscript" => {}
            _ => {
                let mut cursor = node.walk();
                for child in node.named_children(&mut cursor) {
                    self.bind_targets(child);
                }
            }
        }
    }

    fn import(&mut self, node: Node<'_>) {
        let lowered = lower_import(node, self.source, &self.base_package);
        for import in lowered.imports {
            self.note_binding(&import.bound);
            self.decls.imports.push(import);
        }
        self.decls.star_imports.extend(lowered.star);
    }

    /// A module-level or class-body statement.
    fn statement(&mut self, stmt: Node<'_>, class_stack: &[String], owner: Option<usize>) {
        let Some(inner) = stmt.named_child(0) else {
            return;
        };
        match inner.kind() {
            "assignment" => self.assignment(inner, class_stack, owner),
            "augmented_assignment" if class_stack.is_empty() => {
                if let Some(left) = inner.child_by_field_name("left") {
                    self.bind_targets(left);
                }
            }
            _ => {}
        }
    }

    fn assignment(&mut self, node: Node<'_>, class_stack: &[String], owner: Option<usize>) {
        let (Some(left), right, ty) = (
            node.child_by_field_name("left"),
            node.child_by_field_name("right"),
            node.child_by_field_name("type"),
        ) else {
            return;
        };
        let chained = right.filter(|r| r.kind() == "assignment");
        if let Some(inner) = chained {
            self.assignment(inner, class_stack, owner);
        }
        if class_stack.is_empty() {
            if left.kind() != "identifier" {
                self.bind_targets(left);
                return;
            }
            let name = node_text(left, self.source);
            if name == "__all__" {
                self.decls.all = right.and_then(|r| string_list(r, self.source));
            }
            let ty = ty.map(|t| lower_type(t, self.source));
            let value = match (right, chained) {
                (Some(r), None) => Some(lower_expr(r, self.source, &Scope::module(None))),
                (Some(_), Some(_)) => Some(PyExpr::Unknown(Why::Complex)),
                _ => None,
            };
            let bound = right.is_some();
            let acc = self.var(&name);
            acc.is_var = true;
            if ty.is_some() {
                acc.ty = ty;
            }
            if bound {
                acc.count += 1;
                acc.value = value;
            }
        } else if let Some(owner) = owner
            && left.kind() == "identifier"
        {
            let name = node_text(left, self.source);
            let class_qual = self.decls.classes[owner].qualname.clone();
            let source = match (ty, right, chained) {
                (Some(t), _, _) => Some(PyAttrSource::Declared(lower_type(t, self.source))),
                (None, Some(r), None) => Some(PyAttrSource::Value(lower_expr(
                    r,
                    self.source,
                    &Scope::module(Some(&class_qual)),
                ))),
                _ => None,
            };
            if let Some(source) = source {
                self.add_source(owner, &name, source);
            }
        }
    }

    fn add_source(&mut self, class: usize, name: &str, source: PyAttrSource) {
        let attrs = &mut self.decls.classes[class].attrs;
        match attrs.iter_mut().find(|a| a.name == name) {
            Some(attr) => attr.sources.push(source),
            None => attrs.push(PyAttrDecl {
                name: name.to_string(),
                sources: vec![source],
            }),
        }
    }

    fn class(&mut self, node: Node<'_>, class_stack: &mut Vec<String>) {
        let Some(name_node) = node.child_by_field_name("name") else {
            return;
        };
        let name = node_text(name_node, self.source);
        let qualname = qualify(&self.module, class_stack, &name);
        if class_stack.is_empty() {
            self.note_binding(&name);
        }
        let mut bases = Vec::new();
        if let Some(supers) = node.child_by_field_name("superclasses") {
            let mut cursor = supers.walk();
            for child in supers.named_children(&mut cursor) {
                // `metaclass=...` and other keywords are not bases.
                if child.kind() != "keyword_argument" {
                    bases.push(lower_type(child, self.source));
                }
            }
        }
        let idx = self.decls.classes.len();
        self.decls.classes.push(PyClassDecl {
            qualname,
            name: name.clone(),
            bases,
            attrs: Vec::new(),
            start_line: definition_span(node).0,
        });
        if let Some(body) = node.child_by_field_name("body") {
            class_stack.push(name);
            self.walk(body, class_stack, Some(idx));
            class_stack.pop();
        }
    }

    fn function<'t>(
        &mut self,
        node: Node<'t>,
        decorators: &[Node<'t>],
        class_stack: &[String],
        owner: Option<usize>,
    ) {
        let Some(name_node) = node.child_by_field_name("name") else {
            return;
        };
        let name = node_text(name_node, self.source);
        if class_stack.is_empty() {
            self.note_binding(&name);
        }
        let mut is_overload = false;
        let mut kind = if owner.is_some() {
            PyFuncKind::Method
        } else {
            PyFuncKind::Function
        };
        for dec in decorators {
            let Some(expr) = dec.named_child(0) else {
                continue;
            };
            let target = if expr.kind() == "call" {
                expr.child_by_field_name("function").unwrap_or(expr)
            } else {
                expr
            };
            let text = node_text(target, self.source);
            let last = text.rsplit('.').next().unwrap_or(&text);
            match last {
                // `@x.setter` / `@x.deleter` redefine a property, they are
                // not a callable of their own.
                "setter" | "deleter" if text.contains('.') => {
                    self.setter_attrs(node, owner, class_stack);
                    return;
                }
                "overload" => is_overload = true,
                "staticmethod" if owner.is_some() => kind = PyFuncKind::StaticMethod,
                "classmethod" if owner.is_some() => kind = PyFuncKind::ClassMethod,
                "property" | "cached_property" | "abstractproperty" | "getter"
                    if owner.is_some() =>
                {
                    kind = PyFuncKind::Property
                }
                _ => {}
            }
        }
        let params = node
            .child_by_field_name("parameters")
            .map(|p| param_infos(p, self.source))
            .unwrap_or_default();
        let returns = node
            .child_by_field_name("return_type")
            .map(|t| lower_type(t, self.source));
        let qualname = qualify(&self.module, class_stack, &name);
        let returns_self = returns.is_none()
            && owner.is_some()
            && kind == PyFuncKind::Method
            && params
                .first()
                .is_some_and(|p| p.name == "self" && p.star == 0)
            && node
                .child_by_field_name("body")
                .is_some_and(|b| only_returns_self(b, self.source));
        let is_async = {
            let text = self.source.get(node.start_byte()..).unwrap_or("");
            text.starts_with("async") && text[5..].chars().next().is_some_and(char::is_whitespace)
        };
        if let Some(owner) = owner {
            if kind == PyFuncKind::Property
                && let Some(ty) = &returns
            {
                self.add_source(owner, &name, PyAttrSource::Property(ty.clone()));
            }
            self.self_attrs(node, owner, kind, &params);
        }
        self.decls.functions.push(PyFuncDecl {
            qualname,
            name,
            owner: owner.map(|o| o as u32),
            kind,
            params,
            returns,
            is_async,
            is_overload,
            returns_self,
            start_line: definition_span(node).0,
        });
    }

    /// A property setter's body still assigns `self.x`.
    fn setter_attrs(&mut self, node: Node<'_>, owner: Option<usize>, _class_stack: &[String]) {
        if let Some(owner) = owner {
            let params = node
                .child_by_field_name("parameters")
                .map(|p| param_infos(p, self.source))
                .unwrap_or_default();
            self.self_attrs(node, owner, PyFuncKind::Method, &params);
        }
    }

    /// Collect `self.x ...` assignments from one method body.
    fn self_attrs(&mut self, func: Node<'_>, owner: usize, kind: PyFuncKind, params: &[PyParam]) {
        if kind == PyFuncKind::StaticMethod {
            return;
        }
        let Some(first) = params.first().filter(|p| p.star == 0) else {
            return;
        };
        let is_cls = kind == PyFuncKind::ClassMethod;
        let class_qual = self.decls.classes[owner].qualname.clone();
        let scope = Scope::for_function(
            func,
            self.source,
            Some(&class_qual),
            true,
            is_cls,
            &self.base_package,
        );
        let Some(body) = func.child_by_field_name("body") else {
            return;
        };
        let mut found = Vec::new();
        collect_self_assignments(body, self.source, &first.name, &scope, &mut found);
        for (name, source) in found {
            self.add_source(owner, &name, source);
        }
    }
}

/// `self.x = ...` statements anywhere in a method body (not in nested
/// scopes), as `(attr name, source)` in source order.
fn collect_self_assignments(
    node: Node<'_>,
    source: &str,
    self_name: &str,
    scope: &Scope<'_>,
    out: &mut Vec<(String, PyAttrSource)>,
) {
    let is_self_attr = |n: Node<'_>| -> Option<String> {
        if n.kind() != "attribute" {
            return None;
        }
        let object = n.child_by_field_name("object")?;
        if object.kind() != "identifier" || node_text(object, source) != self_name {
            return None;
        }
        Some(node_text(n.child_by_field_name("attribute")?, source))
    };
    match node.kind() {
        "function_definition" | "class_definition" | "lambda" => return,
        "assignment" => {
            let left = node.child_by_field_name("left");
            let right = node.child_by_field_name("right");
            if let Some(left) = left {
                if let Some(name) = is_self_attr(left) {
                    let src = match (node.child_by_field_name("type"), right) {
                        (Some(t), _) => PyAttrSource::Declared(lower_type(t, source)),
                        (None, Some(r)) if r.kind() == "assignment" => {
                            PyAttrSource::Value(PyExpr::Unknown(Why::Complex))
                        }
                        (None, Some(r)) => PyAttrSource::Value(lower_expr(r, source, scope)),
                        (None, None) => return,
                    };
                    out.push((name, src));
                } else if left.kind() != "identifier" {
                    let mut stack = vec![left];
                    while let Some(n) = stack.pop() {
                        if let Some(name) = is_self_attr(n) {
                            out.push((name, PyAttrSource::Value(PyExpr::Unknown(Why::Complex))));
                        } else if n.kind() != "attribute" && n.kind() != "subscript" {
                            let mut c = n.walk();
                            let mut kids: Vec<_> = n.named_children(&mut c).collect();
                            kids.reverse();
                            stack.extend(kids);
                        }
                    }
                }
            }
            if let Some(right) = right {
                collect_self_assignments(right, source, self_name, scope, out);
            }
            return;
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_self_assignments(child, source, self_name, scope, out);
    }
}

/// What one `import` / `from .. import` statement binds.
pub(crate) struct LoweredImport {
    pub imports: Vec<PyImport>,
    /// The absolute module of a `from m import *`.
    pub star: Option<String>,
}

/// Lower an import statement to its bindings, absolutizing relative
/// imports against `base_package`.
pub(crate) fn lower_import(node: Node<'_>, source: &str, base_package: &[String]) -> LoweredImport {
    let mut out = LoweredImport {
        imports: Vec::new(),
        star: None,
    };
    match raw_import(node, source) {
        Some(RawImport::Import(items)) => {
            for (module, alias) in items {
                out.imports.push(match alias {
                    Some(bound) => PyImport {
                        bound,
                        target: module,
                        member: false,
                    },
                    None => {
                        let first = module.split('.').next().unwrap_or(&module).to_string();
                        PyImport {
                            bound: first.clone(),
                            target: first,
                            member: false,
                        }
                    }
                });
            }
        }
        Some(RawImport::From { base, star, items }) => {
            let Some(abs_base) = absolutize_module(&base, base_package) else {
                return out;
            };
            if star {
                out.star = Some(abs_base);
                return out;
            }
            for (item, alias) in items {
                out.imports.push(PyImport {
                    bound: alias.unwrap_or_else(|| item.clone()),
                    target: join_from_import_target(&abs_base, &item),
                    member: true,
                });
            }
        }
        None => {}
    }
    out
}

fn qualify(module: &str, class_stack: &[String], name: &str) -> String {
    if class_stack.is_empty() {
        format!("{module}.{name}")
    } else {
        format!("{module}.{}.{}", class_stack.join("."), name)
    }
}

/// A list/tuple of plain string literals.
fn string_list(node: Node<'_>, source: &str) -> Option<Vec<String>> {
    if !matches!(node.kind(), "list" | "tuple") {
        return None;
    }
    let mut out = Vec::new();
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        if child.kind() != "string" {
            return None;
        }
        let mut text = String::new();
        let mut c = child.walk();
        for part in child.children(&mut c) {
            match part.kind() {
                "string_content" => text.push_str(&node_text(part, source)),
                "string_start" | "string_end" => {}
                _ => return None,
            }
        }
        out.push(text);
    }
    Some(out)
}

/// Whether `body` (a function body) has at least one `return` statement and
/// every one of them is exactly `return self`. Nested defs, classes and
/// lambdas own their returns; a body that yields is a generator.
fn only_returns_self(body: Node<'_>, source: &str) -> bool {
    fn walk(node: Node<'_>, source: &str, found: &mut u32, ok: &mut bool) {
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            match child.kind() {
                "function_definition" | "class_definition" | "lambda" => {}
                "yield" => *ok = false,
                "return_statement" => {
                    *found += 1;
                    let mut c = child.walk();
                    let mut values = child.named_children(&mut c);
                    let exact = matches!(
                        (values.next(), values.next()),
                        (Some(v), None) if v.kind() == "identifier" && node_text(v, source) == "self"
                    );
                    if !exact {
                        *ok = false;
                    }
                    walk(child, source, found, ok);
                }
                _ => walk(child, source, found, ok),
            }
        }
    }
    let (mut found, mut ok) = (0, true);
    walk(body, source, &mut found, &mut ok);
    ok && found > 0
}
