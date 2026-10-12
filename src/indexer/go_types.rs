//! Go receiver typing: the declared type of the local, parameter or field a
//! method is called on, read from one function's own syntax.
//!
//! This is deliberately not a type checker. A binding is typed only where its
//! declaration says the type outright (`name T`, `var x T`, `x := T{}` /
//! `&T{}`, `y.(T)`, a type-switch case) or where the type is *named* by
//! something in another file, which the resolver reads once every symbol
//! exists (`x := NewServer()`, `x := a.Load()`, `a.field`): a [`GoValue`].
//! Anything else (a `range` variable, an index, a channel receive, a type
//! parameter, a predeclared type) is a binding with no type, and a method
//! called on it binds nothing.

use crate::indexer::extract::GoValue;
use crate::indexer::tree_helpers::node_text;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use tree_sitter::Node;

/// Go's predeclared types: no declaration of this repository has their
/// methods, so a call on a value of one never binds.
const PREDECLARED: &[&str] = &[
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

/// One name a function binds, with the byte range it is visible in.
struct Binding {
    name: String,
    /// Visible from this offset (the end of its declaring statement, so
    /// `x := x.Next()` reads the outer `x` on the right).
    visible_from: usize,
    scope_start: usize,
    scope_end: usize,
    ty: Option<GoValue>,
}

/// The typed (and untyped) bindings of one function declaration, closures
/// included, and the file facts typing needs.
#[derive(Default)]
pub(super) struct LocalTypes {
    bindings: Vec<Binding>,
    type_params: HashSet<String>,
    /// The calling file's package directory.
    dir: String,
    /// Import alias -> import path.
    imports: Rc<HashMap<String, String>>,
}

impl LocalTypes {
    /// The package scope: no bindings, so every unbound name is a package
    /// variable (for the initializer of a package-level `var`).
    pub(super) fn package_scope(dir: &str, imports: Rc<HashMap<String, String>>) -> Self {
        LocalTypes {
            dir: dir.to_string(),
            imports,
            ..Default::default()
        }
    }

    /// The declared type text of a type expression, as stored for a
    /// package variable (`None` for a type that cannot be named).
    pub(super) fn type_text(&self, node: Node<'_>, source: &str) -> Option<String> {
        self.type_from(node, source)?;
        let text = node_text(node, source)
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        (!text.contains(['{', '('])).then_some(text)
    }

    /// The bindings of function-like `func` (a declaration or a literal).
    pub(super) fn collect(
        func: Node<'_>,
        source: &str,
        dir: &str,
        imports: Rc<HashMap<String, String>>,
    ) -> Self {
        let mut types = LocalTypes {
            bindings: Vec::new(),
            type_params: HashSet::new(),
            dir: dir.to_string(),
            imports,
        };
        types.collect_type_params(func, source);
        types.bind_params(func, source, func.start_byte(), func.end_byte());
        let mut cursor = func.walk();
        for child in func.named_children(&mut cursor) {
            types.walk(child, source);
        }
        types
    }

    /// `place` of a type declared in the calling file's package.
    fn same_package(&self) -> String {
        format!("{}{}", crate::db::resolver::GO_SAME_PACKAGE, self.dir)
    }

    fn collect_type_params(&mut self, func: Node<'_>, source: &str) {
        if let Some(list) = func.child_by_field_name("type_parameters") {
            collect_names(list, source, &mut self.type_params);
        }
        // `func (s *Set[T]) Add`: the receiver's type arguments name the
        // type's parameters, not types.
        if let Some(receiver) = func.child_by_field_name("receiver") {
            let mut cursor = receiver.walk();
            for param in receiver.named_children(&mut cursor) {
                if let Some(ty) = param.child_by_field_name("type") {
                    let ty = strip_pointer(ty);
                    if ty.kind() == "generic_type"
                        && let Some(args) = ty.child_by_field_name("type_arguments")
                    {
                        collect_names(args, source, &mut self.type_params);
                    }
                }
            }
        }
    }

    /// Bind the receiver, parameters and named results of `func`.
    fn bind_params(&mut self, func: Node<'_>, source: &str, scope_start: usize, scope_end: usize) {
        for field in ["receiver", "parameters", "result"] {
            let Some(list) = func.child_by_field_name(field) else {
                continue;
            };
            if list.kind() != "parameter_list" {
                continue;
            }
            let mut cursor = list.walk();
            for param in list.named_children(&mut cursor) {
                let ty = param
                    .child_by_field_name("type")
                    .and_then(|t| self.type_from(t, source));
                // `args ...T` is a slice of T.
                let ty = if param.kind() == "variadic_parameter_declaration" {
                    ty.map(|t| GoValue::Seq(Box::new(t)))
                } else {
                    ty
                };
                let mut names = param.walk();
                for name in param.children_by_field_name("name", &mut names) {
                    self.bindings.push(Binding {
                        name: node_text(name, source),
                        visible_from: scope_start,
                        scope_start,
                        scope_end,
                        ty: ty.clone(),
                    });
                }
            }
        }
    }

    fn bind(&mut self, name: &str, decl: Node<'_>, ty: Option<GoValue>) {
        if name == "_" {
            return;
        }
        let (scope_start, scope_end) = enclosing_scope(decl);
        self.bindings.push(Binding {
            name: name.to_string(),
            visible_from: decl.end_byte(),
            scope_start,
            scope_end,
            ty,
        });
    }

    fn walk(&mut self, node: Node<'_>, source: &str) {
        match node.kind() {
            "func_literal" => {
                self.bind_params(node, source, node.start_byte(), node.end_byte());
            }
            "short_var_declaration" => {
                if let (Some(left), Some(right)) = (
                    node.child_by_field_name("left"),
                    node.child_by_field_name("right"),
                ) {
                    self.bind_pairs(node, left, right, source);
                }
            }
            "var_spec" => {
                let mut cursor = node.walk();
                let names: Vec<Node<'_>> = node.children_by_field_name("name", &mut cursor).collect();
                let declared = node
                    .child_by_field_name("type")
                    .map(|t| self.type_from(t, source));
                let right = node.child_by_field_name("value");
                for (i, name) in names.iter().enumerate() {
                    let ty = match (&declared, right) {
                        (Some(ty), _) => ty.clone(),
                        (None, Some(right)) => self.pair_type(right, i, names.len(), source),
                        (None, None) => None,
                    };
                    self.bind(&node_text(*name, source), node, ty);
                }
            }
            "range_clause" => {
                if let Some(left) = node.child_by_field_name("left") {
                    let mut cursor = left.walk();
                    let names: Vec<Node<'_>> = left.named_children(&mut cursor).collect();
                    // `for i, v := range xs`: the second variable is an
                    // element (of a slice, array or map).
                    let element = (names.len() == 2)
                        .then(|| node.child_by_field_name("right"))
                        .flatten()
                        .and_then(|right| self.value_type(right, 0, source))
                        .map(GoValue::element);
                    for (i, name) in names.iter().enumerate() {
                        if name.kind() == "identifier" {
                            let ty = if i == 1 { element.clone() } else { None };
                            self.bind(&node_text(*name, source), node, ty);
                        }
                    }
                }
            }
            "receive_statement" => {
                if let Some(left) = node.child_by_field_name("left") {
                    let mut names = HashSet::new();
                    crate::indexer::tree_helpers::collect_identifier_names(
                        left, source, &mut names,
                    );
                    for name in names {
                        self.bind(&name, node, None);
                    }
                }
            }
            "type_switch_statement" => self.bind_type_switch(node, source),
            _ => {}
        }
        let mut cursor = node.walk();
        for child in node.named_children(&mut cursor) {
            self.walk(child, source);
        }
    }

    /// `a, b := x, y` / `a, b := f()` / `a := x`.
    fn bind_pairs(
        &mut self,
        decl: Node<'_>,
        left: Node<'_>,
        right: Node<'_>,
        source: &str,
    ) {
        let mut cursor = left.walk();
        let names: Vec<Node<'_>> = left.named_children(&mut cursor).collect();
        for (i, name) in names.iter().enumerate() {
            if name.kind() != "identifier" {
                continue;
            }
            let ty = self.pair_type(right, i, names.len(), source);
            self.bind(&node_text(*name, source), decl, ty);
        }
    }

    /// The type of the `i`th of `n` names bound from the expression list
    /// `right`: pairwise, or the `i`th result of a single call.
    fn pair_type(&self, right: Node<'_>, i: usize, n: usize, source: &str) -> Option<GoValue> {
        let mut cursor = right.walk();
        let exprs: Vec<Node<'_>> = right.named_children(&mut cursor).collect();
        if exprs.len() == n {
            return self.value_type(exprs[i], 0, source);
        }
        if exprs.len() == 1 {
            return self.value_type(exprs[0], u8::try_from(i).ok()?, source);
        }
        None
    }

    fn bind_type_switch(&mut self, node: Node<'_>, source: &str) {
        let Some(alias) = node.child_by_field_name("alias") else {
            return;
        };
        let mut cursor = alias.walk();
        let Some(name) = alias.named_children(&mut cursor).next() else {
            return;
        };
        let name = node_text(name, source);
        let value = node.child_by_field_name("value");
        let mut cursor = node.walk();
        for case in node.named_children(&mut cursor) {
            let ty = match case.kind() {
                "type_case" => {
                    let mut c = case.walk();
                    let types: Vec<Node<'_>> =
                        case.children_by_field_name("type", &mut c).collect();
                    match types[..] {
                        [only] => self.type_from(only, source),
                        _ => None,
                    }
                }
                "default_case" => value.and_then(|v| self.value_type(v, 0, source)),
                _ => continue,
            };
            self.bindings.push(Binding {
                name: name.clone(),
                visible_from: case.start_byte(),
                scope_start: case.start_byte(),
                scope_end: case.end_byte(),
                ty,
            });
        }
    }

    /// The innermost binding of `name` visible at byte `pos`.
    fn lookup(&self, name: &str, pos: usize) -> Option<&Binding> {
        self.bindings
            .iter()
            .filter(|b| {
                b.name == name
                    && b.visible_from <= pos
                    && b.scope_start <= pos
                    && pos < b.scope_end
            })
            .max_by_key(|b| (b.scope_start, b.visible_from))
    }

    /// Whether `name` is bound by the function at `pos` (so it is no package).
    pub(super) fn is_bound(&self, name: &str, pos: usize) -> bool {
        self.lookup(name, pos).is_some()
    }

    /// The type of a type expression: `T`, `*T`, `pkg.T`, `Set[T]`.
    fn type_from(&self, node: Node<'_>, source: &str) -> Option<GoValue> {
        match node.kind() {
            "type_identifier" | "identifier" => {
                let name = node_text(node, source);
                if PREDECLARED.contains(&name.as_str()) || self.type_params.contains(&name) {
                    return None;
                }
                Some(GoValue::Type {
                    place: self.same_package(),
                    name,
                })
            }
            "pointer_type" | "parenthesized_type" | "parenthesized_expression" => {
                let mut cursor = node.walk();
                let inner = node.named_children(&mut cursor).next()?;
                self.type_from(inner, source)
            }
            "unary_expression" => self.type_from(node.child_by_field_name("operand")?, source),
            "slice_type" | "array_type" | "implicit_length_array_type" => {
                let element = self.type_from(node.child_by_field_name("element")?, source)?;
                Some(GoValue::Seq(Box::new(element)))
            }
            "map_type" => {
                let element = self.type_from(node.child_by_field_name("value")?, source)?;
                Some(GoValue::Seq(Box::new(element)))
            }
            "generic_type" => self.type_from(node.child_by_field_name("type")?, source),
            "qualified_type" => {
                let pkg = node_text(node.child_by_field_name("package")?, source);
                let name = node_text(node.child_by_field_name("name")?, source);
                self.qualified(&pkg, name)
            }
            "selector_expression" => {
                let operand = node.child_by_field_name("operand")?;
                if operand.kind() != "identifier" {
                    return None;
                }
                let pkg = node_text(operand, source);
                let name = node_text(node.child_by_field_name("field")?, source);
                self.qualified(&pkg, name)
            }
            _ => None,
        }
    }

    fn qualified(&self, pkg: &str, name: String) -> Option<GoValue> {
        let path = self.imports.get(pkg)?;
        Some(GoValue::Type {
            place: path.clone(),
            name,
        })
    }

    /// The type of expression `node`, taking result number `index` of a
    /// call. `None`: not nameable here (the receiver stays untyped).
    pub(super) fn value_type(&self, node: Node<'_>, index: u8, source: &str) -> Option<GoValue> {
        match node.kind() {
            "identifier" => {
                if index != 0 {
                    return None;
                }
                let name = node_text(node, source);
                match self.lookup(&name, node.start_byte()) {
                    Some(binding) => binding.ty.clone(),
                    // Not bound by the function: a package-level variable
                    // (of this package, declared in any of its files).
                    None if self.imports.contains_key(&name) => None,
                    None => Some(GoValue::Var {
                        place: self.same_package(),
                        name,
                    }),
                }
            }
            "parenthesized_expression" => {
                let mut cursor = node.walk();
                let inner = node.named_children(&mut cursor).next()?;
                self.value_type(inner, index, source)
            }
            // `&x` and `*x` name the type `x` has (a pointer's methods are
            // its element's).
            "unary_expression" => {
                let op = node.child_by_field_name("operator")?;
                if index != 0 || !matches!(op.kind(), "&" | "*") {
                    return None;
                }
                self.value_type(node.child_by_field_name("operand")?, 0, source)
            }
            "composite_literal" if index == 0 => {
                self.type_from(node.child_by_field_name("type")?, source)
            }
            "type_assertion_expression" if index == 0 => {
                self.type_from(node.child_by_field_name("type")?, source)
            }
            "call_expression" => self.call_type(node, index, source),
            "index_expression" if index == 0 => {
                let of = self.value_type(node.child_by_field_name("operand")?, 0, source)?;
                if of.depth() >= crate::indexer::extract::MAX_DEFERRED_DEPTH {
                    return None;
                }
                Some(GoValue::element(of))
            }
            "slice_expression" if index == 0 => {
                self.value_type(node.child_by_field_name("operand")?, 0, source)
            }
            "selector_expression" if index == 0 => {
                let operand = node.child_by_field_name("operand")?;
                let name = node_text(node.child_by_field_name("field")?, source);
                let owner = self.value_type(operand, 0, source)?;
                if owner.depth() >= crate::indexer::extract::MAX_DEFERRED_DEPTH {
                    return None;
                }
                Some(GoValue::Field {
                    owner: Box::new(owner),
                    name,
                })
            }
            _ => None,
        }
    }

    fn call_type(&self, call: Node<'_>, index: u8, source: &str) -> Option<GoValue> {
        let function = strip_instantiation(call.child_by_field_name("function")?);
        match function.kind() {
            "identifier" => {
                let name = node_text(function, source);
                if self.lookup(&name, function.start_byte()).is_some() {
                    // A called local (`f := func() ..; f()`) has no declaration.
                    return None;
                }
                match name.as_str() {
                    // `new(T)` is a `*T`; `make(T, ..)` a `T`.
                    "new" | "make" if index == 0 => {
                        let arg = call.child_by_field_name("arguments")?;
                        let mut cursor = arg.walk();
                        let first = arg.named_children(&mut cursor).next()?;
                        self.type_from(first, source)
                    }
                    // `append(xs, ..)` is the slice it grows.
                    "append" if index == 0 => {
                        let arg = call.child_by_field_name("arguments")?;
                        let mut cursor = arg.walk();
                        let first = arg.named_children(&mut cursor).next()?;
                        self.value_type(first, 0, source)
                    }
                    n if is_builtin_name(n) => None,
                    _ => Some(GoValue::Func {
                        place: self.same_package(),
                        name,
                        index,
                    }),
                }
            }
            "selector_expression" => {
                let operand = function.child_by_field_name("operand")?;
                let name = node_text(function.child_by_field_name("field")?, source);
                if operand.kind() == "identifier" {
                    let pkg = node_text(operand, source);
                    if self.lookup(&pkg, operand.start_byte()).is_none()
                        && let Some(path) = self.imports.get(&pkg)
                    {
                        return Some(GoValue::Func {
                            place: path.clone(),
                            name,
                            index,
                        });
                    }
                }
                let recv = self.value_type(operand, 0, source)?;
                if recv.depth() >= crate::indexer::extract::MAX_DEFERRED_DEPTH {
                    return None;
                }
                Some(GoValue::Method {
                    recv: Box::new(recv),
                    name,
                    index,
                })
            }
            // `(*T)(x)` converts.
            "parenthesized_expression" if index == 0 => self.type_from(function, source),
            _ => None,
        }
    }
}

fn is_builtin_name(name: &str) -> bool {
    matches!(
        name,
        "append"
            | "len"
            | "cap"
            | "make"
            | "new"
            | "delete"
            | "copy"
            | "panic"
            | "recover"
            | "print"
            | "println"
            | "close"
            | "min"
            | "max"
            | "clear"
            | "complex"
            | "real"
            | "imag"
    ) || PREDECLARED.contains(&name)
}

/// The callee of a generic instantiation without its type arguments.
fn strip_instantiation(node: Node<'_>) -> Node<'_> {
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

fn strip_pointer(node: Node<'_>) -> Node<'_> {
    let mut node = node;
    while node.kind() == "pointer_type" {
        let mut cursor = node.walk();
        match node.named_children(&mut cursor).next() {
            Some(inner) => node = inner,
            None => break,
        }
    }
    node
}

/// Every identifier under `node`.
fn collect_names(node: Node<'_>, source: &str, out: &mut HashSet<String>) {
    if matches!(node.kind(), "identifier" | "type_identifier") {
        out.insert(node_text(node, source));
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_names(child, source, out);
    }
}

/// The byte range of the innermost block-like node holding `node`.
fn enclosing_scope(node: Node<'_>) -> (usize, usize) {
    let mut parent = node.parent();
    while let Some(p) = parent {
        if matches!(
            p.kind(),
            "block"
                | "if_statement"
                | "for_statement"
                | "expression_switch_statement"
                | "type_switch_statement"
                | "expression_case"
                | "default_case"
                | "type_case"
                | "communication_case"
                | "func_literal"
                | "function_declaration"
                | "method_declaration"
                | "source_file"
        ) {
            return (p.start_byte(), p.end_byte());
        }
        parent = p.parent();
    }
    (node.start_byte(), node.end_byte())
}
