//! Lowering Python expressions to [`PyExpr`].
//!
//! A [`Scope`] describes the names a function body binds (parameters and
//! locals); [`lower_expr`] turns any expression node into a self-contained
//! [`PyExpr`], inlining locals bound exactly once. Module-level code and
//! class bodies are scopes too ([`Scope::for_module`],
//! [`Scope::for_class_body`]). [`lower_call_site`] lowers one `call` node to
//! a [`PyCallSite`]. This is the single place that interprets expression
//! syntax.
//!
//! Scoping rules:
//! - A name bound exactly once (assignment, annotated assignment,
//!   `with .. as`, walrus) is inlined as its value; an annotation wins over
//!   the value. Anything else a body binds becomes `Unknown(..)` with the
//!   reason, so a call on it never falls back to a bare-name guess.
//! - A function-local import binds [`PyExpr::Imported`]. At module level an
//!   import (and a `def` / `class`) stays a free [`PyExpr::Name`], resolved
//!   at evaluation against the module's declarations.
//! - A nested `def` sees its enclosing function's names (closure); a class
//!   body's names are not visible to the methods inside it. Lambda
//!   parameters and comprehension targets shadow, as `Unknown`.
//! - Module-level code inlines module names bound once; functions leave
//!   module names free (a `global` statement anywhere marks the name
//!   `Unknown(Global)` at module level).

use crate::indexer::python::parse_import_bindings;
use crate::indexer::python_expr::{MAX_BUDGET, MAX_DEPTH, PyCallSite, PyExpr, Why};
use crate::indexer::python_types::{PyTypeRef, lower_import, lower_type, param_infos};
use crate::indexer::tree_helpers::node_text;
use std::collections::HashMap;
use std::rc::Rc;
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
    /// Bound several times, every time by a simple assignment: the value is
    /// known only when all of them agree (evaluation decides).
    Many(Vec<Node<'t>>),
    /// Bound exactly once as the `as` target of a `with`.
    Enter(Node<'t>, bool),
    /// Annotated: the annotation wins over any value.
    Declared(PyTypeRef),
    /// Bound by a function-local import to this absolute target.
    Imported(String),
    /// Bound by a module-level import to this absolute target; stays a
    /// free name (the module's declarations resolve it).
    ModuleImport(String),
    /// Bound by a module-level `def` / `class`; stays a free name.
    Free,
    Unknown(Why),
}

/// The kind of body whose bindings are being collected.
#[derive(Clone, Copy)]
enum Level<'a> {
    /// A function body; carries the base package for relative imports.
    Function(&'a [String]),
    Module(&'a [String]),
    Class,
}

/// The names one function body (or module / class body) binds.
pub struct Scope<'t> {
    /// Enclosing class qualname, when inside a method (or class body).
    class: Option<String>,
    is_cls: bool,
    is_class_body: bool,
    bindings: HashMap<String, Binding<'t>>,
    /// The scope this one sits in: closure names of a nested `def`, the
    /// enclosing function of a lambda or comprehension.
    outer: Option<Rc<Scope<'t>>>,
    /// What a `yield` in this function evaluates to: the send type of its
    /// `Generator[Y, S, R]` / `AsyncGenerator[Y, S]` return annotation.
    yield_send: Option<PyTypeRef>,
}

impl<'t> Scope<'t> {
    /// A scope that binds nothing: names stay free.
    pub fn module(class: Option<&str>) -> Self {
        Self {
            class: class.map(str::to_string),
            is_cls: false,
            is_class_body: false,
            bindings: HashMap::new(),
            outer: None,
            yield_send: None,
        }
    }

    /// The scope of the module's top-level code.
    pub fn for_module(root: Node<'t>, source: &str, base_package: &[String]) -> Self {
        let mut scope = Self::module(None);
        let mut annotated = HashMap::new();
        let mut cursor = root.walk();
        for child in root.named_children(&mut cursor) {
            collect_bindings(
                child,
                source,
                Level::Module(base_package),
                &mut scope.bindings,
                &mut annotated,
            );
        }
        scope.apply_annotations(annotated);
        // A function elsewhere may rebind a module name through `global`.
        let mut globals = Vec::new();
        collect_globals(root, source, &mut globals);
        for name in globals {
            scope.bindings.insert(name, Binding::Unknown(Why::Global));
        }
        scope
    }

    /// The scope of a class body (`class` is the class qualname, `None` for
    /// a class defined inside a function).
    /// `enclosing` is the function scope a class defined inside a function
    /// sits in.
    pub fn for_class_body(
        body: Node<'t>,
        source: &str,
        class: Option<&str>,
        enclosing: Option<&Rc<Scope<'t>>>,
    ) -> Self {
        let mut scope = Self::module(class);
        scope.is_class_body = true;
        scope.outer = enclosing.cloned();
        let mut annotated = HashMap::new();
        let mut cursor = body.walk();
        for child in body.named_children(&mut cursor) {
            collect_bindings(
                child,
                source,
                Level::Class,
                &mut scope.bindings,
                &mut annotated,
            );
        }
        scope.apply_annotations(annotated);
        scope
    }

    /// The scope of `func`'s body. `class` is the enclosing class qualname
    /// (`None` for a plain function); `has_self` says the first parameter
    /// is `self` / `cls` (false for static methods and plain functions),
    /// and `is_cls` that it is `cls`. `base_package` absolutizes relative
    /// function-local imports.
    pub fn for_function(
        func: Node<'t>,
        source: &str,
        class: Option<&str>,
        has_self: bool,
        is_cls: bool,
        base_package: &[String],
    ) -> Self {
        let mut scope = Self {
            class: class.map(str::to_string),
            is_cls,
            is_class_body: false,
            bindings: HashMap::new(),
            outer: None,
            yield_send: func
                .child_by_field_name("return_type")
                .and_then(|t| yield_send_type(lower_type(t, source))),
        };
        let mut first = true;
        if let Some(params) = func.child_by_field_name("parameters") {
            for param in param_infos(params, source) {
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
            collect_bindings(
                body,
                source,
                Level::Function(base_package),
                &mut scope.bindings,
                &mut annotated,
            );
            scope.apply_annotations(annotated);
        }
        scope
    }

    /// Make this the scope of a function nested in `outer`: names it does
    /// not bind itself resolve through `outer`, skipping class bodies.
    pub fn nested_in(mut self, outer: &Rc<Scope<'t>>) -> Self {
        let mut o = Some(outer.clone());
        while let Some(s) = o.clone() {
            if !s.is_class_body {
                break;
            }
            o = s.outer.clone();
        }
        self.outer = o;
        self
    }

    fn apply_annotations(&mut self, annotated: HashMap<String, PyTypeRef>) {
        for (name, ty) in annotated {
            if !matches!(
                self.bindings.get(&name),
                Some(Binding::Unknown(Why::Global | Why::Nonlocal) | Binding::SelfParam)
            ) {
                self.bindings.insert(name, Binding::Declared(ty));
            }
        }
    }

    /// The name of the class this scope sits in, if any.
    pub fn class(&self) -> Option<&str> {
        self.class.as_deref()
    }

    /// The binding of `name` and the scope that owns it.
    fn lookup(&self, name: &str) -> Option<(&Binding<'t>, &Scope<'t>)> {
        match self.bindings.get(name) {
            Some(b) => Some((b, self)),
            None => self.outer.as_deref()?.lookup(name),
        }
    }

    /// The scope a call inside `base` sees: `base` plus the parameters of
    /// every lambda and the targets of every comprehension enclosing `call`
    /// within the same function.
    pub fn for_call(base: &Rc<Scope<'t>>, call: Node<'t>, source: &str) -> Rc<Scope<'t>> {
        let mut layers = Vec::new();
        let mut child = call;
        while let Some(parent) = child.parent() {
            match parent.kind() {
                "function_definition" | "class_definition" | "decorated_definition" | "module" => {
                    break;
                }
                "lambda" => {
                    // Defaults evaluate in the enclosing scope.
                    if parent
                        .child_by_field_name("body")
                        .is_some_and(|b| b.id() == child.id())
                    {
                        layers.push(parent);
                    }
                }
                "list_comprehension"
                | "set_comprehension"
                | "dictionary_comprehension"
                | "generator_expression" => layers.push(parent),
                _ => {}
            }
            child = parent;
        }
        let mut scope = base.clone();
        for node in layers.into_iter().rev() {
            let mut bindings = HashMap::new();
            if node.kind() == "lambda" {
                if let Some(params) = node.child_by_field_name("parameters") {
                    for p in param_infos(params, source) {
                        bindings.insert(p.name, Binding::Unknown(Why::Lambda));
                    }
                }
            } else {
                let mut cursor = node.walk();
                for clause in node.named_children(&mut cursor) {
                    if clause.kind() == "for_in_clause"
                        && let Some(left) = clause.child_by_field_name("left")
                    {
                        bind_all_identifiers(left, source, &mut bindings, Why::Comprehension);
                    }
                }
            }
            if bindings.is_empty() {
                continue;
            }
            scope = Rc::new(Scope {
                class: scope.class.clone(),
                is_cls: scope.is_cls,
                is_class_body: false,
                bindings,
                yield_send: scope.yield_send.clone(),
                outer: Some(scope),
            });
        }
        scope
    }
}

fn bind<'t>(bindings: &mut HashMap<String, Binding<'t>>, name: String, binding: Binding<'t>) {
    match (bindings.get(&name), &binding) {
        (None, _) => {
            bindings.insert(name, binding);
        }
        (Some(Binding::Unknown(Why::Global | Why::Nonlocal)), _) => {}
        // `import a` and `import a.b` both bind `a` to the same target.
        (Some(Binding::Imported(a)), Binding::Imported(b))
        | (Some(Binding::ModuleImport(a)), Binding::ModuleImport(b))
            if a == b => {}
        (Some(Binding::Free), Binding::Free) => {}
        (Some(Binding::Once(a)), Binding::Once(b)) => {
            let many = Binding::Many(vec![*a, *b]);
            bindings.insert(name, many);
        }
        (Some(Binding::Many(_)), Binding::Once(b)) => {
            if let Some(Binding::Many(nodes)) = bindings.get_mut(&name) {
                nodes.push(*b);
            }
        }
        (Some(_), _) => {
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

fn collect_globals(node: Node<'_>, source: &str, out: &mut Vec<String>) {
    if node.kind() == "global_statement" {
        let mut cursor = node.walk();
        out.extend(
            node.named_children(&mut cursor)
                .map(|id| node_text(id, source)),
        );
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_globals(child, source, out);
    }
}

/// Walruses inside a comprehension or lambda-free expression bind in the
/// enclosing function; their value cannot be followed.
fn collect_walruses<'t>(node: Node<'t>, source: &str, bindings: &mut HashMap<String, Binding<'t>>) {
    if node.kind() == "named_expression"
        && let Some(name) = node.child_by_field_name("name")
    {
        bind(
            bindings,
            node_text(name, source),
            Binding::Unknown(Why::Comprehension),
        );
    }
    if node.kind() == "lambda" {
        return;
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_walruses(child, source, bindings);
    }
}

/// Record every name `node` (a statement or expression inside a function
/// body) binds. Nested `def`/`class`/`lambda`/comprehensions are scopes of
/// their own: only their own name leaks.
fn collect_bindings<'t>(
    node: Node<'t>,
    source: &str,
    level: Level<'_>,
    bindings: &mut HashMap<String, Binding<'t>>,
    annotated: &mut HashMap<String, PyTypeRef>,
) {
    match node.kind() {
        "function_definition" | "class_definition" => {
            if let Some(name) = node.child_by_field_name("name") {
                let binding = match level {
                    Level::Module(_) => Binding::Free,
                    _ => Binding::Unknown(Why::Complex),
                };
                bind(bindings, node_text(name, source), binding);
            }
            return;
        }
        "lambda" => return,
        "list_comprehension"
        | "set_comprehension"
        | "dictionary_comprehension"
        | "generator_expression" => {
            collect_walruses(node, source, bindings);
            return;
        }
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
                collect_bindings(right, source, level, bindings, annotated);
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
                collect_bindings(value, source, level, bindings, annotated);
            }
            return;
        }
        "for_statement" => {
            if let Some(left) = node.child_by_field_name("left") {
                bind_all_identifiers(left, source, bindings, Why::LoopVar);
            }
        }
        "case_clause" => {
            // A capture pattern binds a name; class patterns name classes.
            // Mark every identifier a pattern mentions as unknown.
            let mut cursor = node.walk();
            for child in node.named_children(&mut cursor) {
                if child.kind() == "case_pattern" {
                    bind_all_identifiers(child, source, bindings, Why::Complex);
                }
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
                    collect_bindings(inner, source, level, bindings, annotated);
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
            // Module-level `global` is a no-op; functions elsewhere are
            // handled by `collect_globals`.
            if !matches!(level, Level::Module(_)) {
                let mut cursor = node.walk();
                for id in node.named_children(&mut cursor) {
                    bindings.insert(node_text(id, source), Binding::Unknown(why));
                }
            }
            return;
        }
        "import_statement" | "import_from_statement" => {
            match level {
                Level::Function(base_package) => {
                    let lowered = lower_import(node, source, base_package);
                    if lowered.imports.is_empty() {
                        // A star import or an import past the package root:
                        // whatever it binds is untracked.
                        for (bound, _) in parse_import_bindings(node, source) {
                            bind(bindings, bound, Binding::Unknown(Why::Untracked));
                        }
                    }
                    for import in lowered.imports {
                        bind(bindings, import.bound, Binding::Imported(import.target));
                    }
                }
                Level::Module(base_package) => {
                    let lowered = lower_import(node, source, base_package);
                    if lowered.imports.is_empty() {
                        for (bound, _) in parse_import_bindings(node, source) {
                            bind(bindings, bound, Binding::Unknown(Why::Untracked));
                        }
                    }
                    for import in lowered.imports {
                        bind(bindings, import.bound, Binding::ModuleImport(import.target));
                    }
                }
                Level::Class => {
                    for (bound, _) in parse_import_bindings(node, source) {
                        bind(bindings, bound, Binding::Unknown(Why::Untracked));
                    }
                }
            }
            return;
        }
        _ => {}
    }
    let mut cursor = node.walk();
    for child in node.named_children(&mut cursor) {
        collect_bindings(child, source, level, bindings, annotated);
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
                && scope.lookup("super").is_none()
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
        "yield" => match &scope.yield_send {
            Some(send) => PyExpr::Declared(send.clone()),
            None => PyExpr::Unknown(Why::Yield),
        },
        _ => PyExpr::Unknown(Why::Complex),
    }
}

/// The send type of a generator return annotation: the second argument of
/// `Generator[Y, S, R]` or `AsyncGenerator[Y, S]` (any module prefix).
fn yield_send_type(ret: PyTypeRef) -> Option<PyTypeRef> {
    let PyTypeRef::Name { path, mut args } = ret else {
        return None;
    };
    match path.last().map(String::as_str) {
        Some("Generator" | "AsyncGenerator") if args.len() >= 2 => Some(args.swap_remove(1)),
        _ => None,
    }
}

fn lower_name(
    name: &str,
    source: &str,
    scope: &Scope<'_>,
    depth: usize,
    budget: &mut usize,
) -> PyExpr {
    let Some((binding, owner)) = scope.lookup(name) else {
        return PyExpr::Name(name.to_string());
    };
    match binding {
        Binding::Free | Binding::ModuleImport(_) => PyExpr::Name(name.to_string()),
        Binding::SelfParam => match owner.class() {
            Some(class) => PyExpr::SelfRef {
                class: class.to_string(),
                cls: owner.is_cls,
            },
            None => PyExpr::Unknown(Why::Untracked),
        },
        Binding::Param(Some(ty)) | Binding::Declared(ty) => PyExpr::Declared(ty.clone()),
        Binding::Param(None) => PyExpr::Unknown(Why::Untracked),
        Binding::Once(value) => lower(*value, source, owner, depth + 1, budget),
        Binding::Many(values) => {
            let mut lowered: Vec<PyExpr> = Vec::new();
            for value in values {
                let e = lower(*value, source, owner, depth + 1, budget);
                if !lowered.contains(&e) {
                    lowered.push(e);
                }
            }
            if lowered.len() == 1 {
                lowered.remove(0)
            } else {
                PyExpr::Agree(lowered)
            }
        }
        Binding::Enter(value, is_async) => PyExpr::Enter {
            value: Box::new(lower(*value, source, owner, depth + 1, budget)),
            is_async: *is_async,
        },
        Binding::Imported(target) => PyExpr::Imported(target.clone()),
        Binding::Unknown(why) => PyExpr::Unknown(*why),
    }
}

/// Lower one `call` node to its call site: the callee expression, with
/// locals inlined, and the argument count.
pub fn lower_call_site(call: Node<'_>, source: &str, scope: &Scope<'_>) -> PyCallSite {
    let callee = match call.child_by_field_name("function") {
        Some(function) => lower_expr(function, source, scope),
        None => PyExpr::Unknown(Why::Complex),
    };
    let arg_count = match call.child_by_field_name("arguments") {
        Some(args) if args.kind() == "argument_list" => {
            let mut cursor = args.walk();
            args.named_children(&mut cursor)
                .filter(|a| a.kind() != "comment")
                .count() as u32
        }
        // A lone generator expression is the one argument.
        Some(_) => 1,
        None => 0,
    };
    PyCallSite { callee, arg_count }
}
