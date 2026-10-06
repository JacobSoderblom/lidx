//! Lowering Python expressions to [`PyExpr`].
//!
//! A [`Scope`] describes the names a function body binds (parameters and
//! locals); [`lower_expr`] turns any expression node into a self-contained
//! [`PyExpr`], inlining locals bound exactly once. Module-level code and
//! class bodies are scopes too ([`Scope::module`]): their names stay free
//! and resolve at evaluation. This is the single place that interprets
//! expression syntax, so call-site lowering reuses it.

use crate::indexer::python::parse_import_bindings;
use crate::indexer::python_expr::{MAX_BUDGET, MAX_DEPTH, PyExpr, Why};
use crate::indexer::python_types::{PyTypeRef, lower_type};
use crate::indexer::tree_helpers::node_text;
use std::collections::HashMap;
use tree_sitter::Node;

/// What a name means inside a scope.
#[derive(Clone)]
enum Binding<'t> {
    /// A parameter, with its annotation.
    Param(Option<PyTypeRef>),
    /// The method's `self` / `cls` parameter.
    SelfParam,
    /// Bound exactly once by a simple assignment (or walrus) to this value.
    Once(Node<'t>),
    /// Bound exactly once as the `as` target of a `with`.
    Enter(Node<'t>, bool),
    /// Annotated: the annotation wins over any value.
    Declared(PyTypeRef),
    Unknown(Why),
}

/// The names one function body (or module / class body) binds.
pub struct Scope<'t> {
    /// Enclosing class qualname, when inside a method (or class body).
    class: Option<String>,
    is_cls: bool,
    bindings: HashMap<String, Binding<'t>>,
}

impl<'t> Scope<'t> {
    /// A scope that binds nothing: module level, class bodies.
    pub fn module(class: Option<&str>) -> Self {
        Self {
            class: class.map(str::to_string),
            is_cls: false,
            bindings: HashMap::new(),
        }
    }

    /// The scope of `func`'s body. `class` is the enclosing class qualname
    /// (`None` for a plain function); `has_self` says the first parameter
    /// is `self` / `cls` (false for static methods and plain functions),
    /// and `is_cls` that it is `cls`.
    pub fn for_function(
        func: Node<'t>,
        source: &str,
        class: Option<&str>,
        has_self: bool,
        is_cls: bool,
    ) -> Self {
        let mut scope = Self {
            class: class.map(str::to_string),
            is_cls,
            bindings: HashMap::new(),
        };
        let mut first = true;
        if let Some(params) = func.child_by_field_name("parameters") {
            for param in crate::indexer::python_types::param_infos(params, source) {
                let binding = if first && has_self && class.is_some() && param.star == 0 {
                    Binding::SelfParam
                } else {
                    Binding::Param(param.ty)
                };
                first = false;
                scope.bindings.insert(param.name, binding);
            }
        }
        if let Some(body) = func.child_by_field_name("body") {
            let mut annotated = HashMap::new();
            collect_bindings(body, source, &mut scope.bindings, &mut annotated);
            for (name, ty) in annotated {
                if !matches!(
                    scope.bindings.get(&name),
                    Some(Binding::Unknown(Why::Global | Why::Nonlocal) | Binding::SelfParam)
                ) {
                    scope.bindings.insert(name, Binding::Declared(ty));
                }
            }
        }
        scope
    }

    /// The name of the class this scope sits in, if any.
    pub fn class(&self) -> Option<&str> {
        self.class.as_deref()
    }
}

fn bind<'t>(bindings: &mut HashMap<String, Binding<'t>>, name: String, binding: Binding<'t>) {
    match bindings.get(&name) {
        None => {
            bindings.insert(name, binding);
        }
        Some(Binding::Unknown(Why::Global | Why::Nonlocal)) => {}
        Some(_) => {
            bindings.insert(name, Binding::Unknown(Why::Rebound));
        }
    }
}

fn bind_all_identifiers<'t>(
    node: Node<'t>,
    source: &str,
    bindings: &mut HashMap<String, Binding<'t>>,
    why: Why,
) {
    match node.kind() {
        "identifier" => bind(bindings, node_text(node, source), Binding::Unknown(why)),
        "attribute" | "subscript" => {}
        _ => {
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                bind_all_identifiers(child, source, bindings, why);
            }
        }
    }
}

/// Record every name `node` (a statement or expression inside a function
/// body) binds. Nested `def`/`class`/`lambda`/comprehensions are scopes of
/// their own: only their own name leaks.
fn collect_bindings<'t>(
    node: Node<'t>,
    source: &str,
    bindings: &mut HashMap<String, Binding<'t>>,
    annotated: &mut HashMap<String, PyTypeRef>,
) {
    match node.kind() {
        "function_definition" | "class_definition" => {
            if let Some(name) = node.child_by_field_name("name") {
                bind(
                    bindings,
                    node_text(name, source),
                    Binding::Unknown(Why::Complex),
                );
            }
            return;
        }
        "lambda"
        | "list_comprehension"
        | "set_comprehension"
        | "dictionary_comprehension"
        | "generator_expression" => return,
        "assignment" => {
            let left = node.child_by_field_name("left");
            let right = node.child_by_field_name("right");
            let ty = node.child_by_field_name("type");
            if let Some(left) = left {
                match left.kind() {
                    "identifier" => {
                        let name = node_text(left, source);
                        if let Some(ty) = ty {
                            annotated.insert(name.clone(), lower_type(ty, source));
                        }
                        match right {
                            Some(r) if r.kind() == "assignment" => {
                                bind(bindings, name, Binding::Unknown(Why::Complex));
                            }
                            Some(r) => bind(bindings, name, Binding::Once(r)),
                            // `x: T` alone is a declaration, not a binding.
                            None => {}
                        }
                    }
                    "attribute" | "subscript" => {}
                    _ => bind_all_identifiers(left, source, bindings, Why::Complex),
                }
            }
            if let Some(right) = right {
                collect_bindings(right, source, bindings, annotated);
            }
            return;
        }
        "augmented_assignment" => {
            if let Some(left) = node.child_by_field_name("left")
                && left.kind() == "identifier"
            {
                bind(
                    bindings,
                    node_text(left, source),
                    Binding::Unknown(Why::Rebound),
                );
                // `bind` leaves a first binding alone; `x += 1` is always one more.
                if !matches!(
                    bindings.get(&node_text(left, source)),
                    Some(Binding::Unknown(Why::Global | Why::Nonlocal))
                ) {
                    bindings.insert(node_text(left, source), Binding::Unknown(Why::Rebound));
                }
            }
        }
        "named_expression" => {
            if let (Some(name), Some(value)) = (
                node.child_by_field_name("name"),
                node.child_by_field_name("value"),
            ) {
                bind(bindings, node_text(name, source), Binding::Once(value));
                collect_bindings(value, source, bindings, annotated);
            }
            return;
        }
        "for_statement" => {
            if let Some(left) = node.child_by_field_name("left") {
                bind_all_identifiers(left, source, bindings, Why::LoopVar);
            }
        }
        "with_item" => {
            if let Some(value) = node.child_by_field_name("value")
                && value.kind() == "as_pattern"
            {
                let is_async = node
                    .parent()
                    .and_then(|p| p.parent())
                    .is_some_and(|w| node_text(w, source).starts_with("async"));
                let mut cursor = value.walk();
                let inner = value.named_children(&mut cursor).next();
                let target = value.child_by_field_name("alias");
                if let (Some(inner), Some(target)) = (inner, target) {
                    let mut tc = target.walk();
                    let mut ids = target.named_children(&mut tc);
                    let first = ids.next();
                    let simple = first.filter(|f| f.kind() == "identifier" && ids.next().is_none());
                    match simple {
                        Some(id) => bind(
                            bindings,
                            node_text(id, source),
                            Binding::Enter(inner, is_async),
                        ),
                        None => bind_all_identifiers(target, source, bindings, Why::Complex),
                    }
                    collect_bindings(inner, source, bindings, annotated);
                }
                return;
            }
        }
        "except_clause" => {
            if let Some(value) = node.child_by_field_name("value")
                && value.kind() == "as_pattern"
                && let Some(alias) = value.child_by_field_name("alias")
            {
                bind_all_identifiers(alias, source, bindings, Why::Complex);
            }
        }
        "global_statement" | "nonlocal_statement" => {
            let why = if node.kind() == "global_statement" {
                Why::Global
            } else {
                Why::Nonlocal
            };
            let mut cursor = node.walk();
            for id in node.named_children(&mut cursor) {
                bindings.insert(node_text(id, source), Binding::Unknown(why));
            }
            return;
        }
        "import_statement" | "import_from_statement" => {
            for (bound, _) in parse_import_bindings(node, source) {
                bind(bindings, bound, Binding::Unknown(Why::Untracked));
            }
            return;
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_bindings(child, source, bindings, annotated);
    }
}

/// Lower `node` in `scope`, with a fresh node budget.
pub fn lower_expr(node: Node<'_>, source: &str, scope: &Scope<'_>) -> PyExpr {
    let mut budget = MAX_BUDGET;
    lower(node, source, scope, 0, &mut budget)
}

fn lower(
    node: Node<'_>,
    source: &str,
    scope: &Scope<'_>,
    depth: usize,
    budget: &mut usize,
) -> PyExpr {
    if depth > MAX_DEPTH {
        return PyExpr::Unknown(Why::Depth);
    }
    if *budget == 0 {
        return PyExpr::Unknown(Why::Budget);
    }
    *budget -= 1;
    let next = |n: Node<'_>, budget: &mut usize| lower(n, source, scope, depth + 1, budget);
    match node.kind() {
        "identifier" => lower_name(&node_text(node, source), source, scope, depth, budget),
        "attribute" => match (
            node.child_by_field_name("object"),
            node.child_by_field_name("attribute"),
        ) {
            (Some(object), Some(attr)) => {
                PyExpr::Attr(Box::new(next(object, budget)), node_text(attr, source))
            }
            _ => PyExpr::Unknown(Why::Complex),
        },
        "call" => {
            let Some(function) = node.child_by_field_name("function") else {
                return PyExpr::Unknown(Why::Complex);
            };
            if function.kind() == "identifier"
                && node_text(function, source) == "super"
                && !scope.bindings.contains_key("super")
                && let Some(class) = scope.class()
            {
                return PyExpr::Super {
                    class: class.to_string(),
                };
            }
            PyExpr::Call(Box::new(next(function, budget)))
        }
        "await" => match node.named_child(0) {
            Some(inner) => PyExpr::Await(Box::new(next(inner, budget))),
            None => PyExpr::Unknown(Why::Complex),
        },
        "parenthesized_expression" => match node.named_child(0) {
            Some(inner) => lower(inner, source, scope, depth, budget),
            None => PyExpr::Unknown(Why::Complex),
        },
        "string"
        | "concatenated_string"
        | "integer"
        | "float"
        | "true"
        | "false"
        | "none"
        | "list"
        | "tuple"
        | "dictionary"
        | "set"
        | "ellipsis" => PyExpr::Unknown(Why::Literal),
        "subscript" => PyExpr::Unknown(Why::Subscript),
        "lambda" => PyExpr::Unknown(Why::Lambda),
        "list_comprehension"
        | "set_comprehension"
        | "dictionary_comprehension"
        | "generator_expression" => PyExpr::Unknown(Why::Comprehension),
        "yield" => PyExpr::Unknown(Why::Yield),
        _ => PyExpr::Unknown(Why::Complex),
    }
}

fn lower_name(
    name: &str,
    source: &str,
    scope: &Scope<'_>,
    depth: usize,
    budget: &mut usize,
) -> PyExpr {
    match scope.bindings.get(name) {
        None => PyExpr::Name(name.to_string()),
        Some(Binding::SelfParam) => match scope.class() {
            Some(class) => PyExpr::SelfRef {
                class: class.to_string(),
                cls: scope.is_cls,
            },
            None => PyExpr::Unknown(Why::Untracked),
        },
        Some(Binding::Param(Some(ty))) | Some(Binding::Declared(ty)) => {
            PyExpr::Declared(ty.clone())
        }
        Some(Binding::Param(None)) => PyExpr::Unknown(Why::Untracked),
        Some(Binding::Once(value)) => lower(*value, source, scope, depth + 1, budget),
        Some(Binding::Enter(value, is_async)) => PyExpr::Enter {
            value: Box::new(lower(*value, source, scope, depth + 1, budget)),
            is_async: *is_async,
        },
        Some(Binding::Unknown(why)) => PyExpr::Unknown(*why),
    }
}
