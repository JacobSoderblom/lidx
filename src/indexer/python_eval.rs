//! Static evaluation of Python expressions over a repo-wide type table.
//!
//! A [`PyTypeTable`] indexes every file's [`PyFileDecls`] (modules by name,
//! classes and functions by qualname). [`PyEval::type_of`] evaluates a
//! lowered [`PyExpr`] to a [`Value`] in the scope of one module, and
//! [`PyEval::resolve_site`] turns a [`PyCallSite`] into an [`Outcome`].
//!
//! The evaluator is precision-first: anything it cannot prove is
//! `Value::Unknown(Why)` and a site on it is never bound. It follows
//! Python's own lookup order:
//!
//! * a free name is the module's own def/class/var, then its imports, then
//!   its star imports (honoring `__all__`), then builtins (external);
//! * a module member is a name the module defines or imports (re-export
//!   chains are followed), and only then a submodule of that name, so a
//!   package `__init__` that re-exports `fn` wins over a `fn.py` next to it;
//! * a class member is found by an MRO walk (C3, left-first fallback) over
//!   bases resolved in the declaring file's scope.
//!
//! Nothing here touches the database; the resolver wiring is a later phase.

use crate::indexer::python_expr::{PyCallSite, PyExpr, Why as LowerWhy};
use crate::indexer::python_types::{
    PyAttrSource, PyClassDecl, PyFileDecls, PyFuncDecl, PyFuncKind, PyImport, PyLoadedFile,
    PyTypeRef, PyVarDecl,
};
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

/// Deepest chain of module re-exports followed.
const MAX_MEMBER_DEPTH: usize = 8;
/// Deepest nesting of `eval` calls (attribute sources reach back into
/// other expressions).
const MAX_EVAL_DEPTH: usize = 48;

/// Why the evaluator could not produce a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Why {
    /// Lowering already gave up.
    Lowered(LowerWhy),
    /// A free name found nowhere (and not a builtin).
    UnresolvedName,
    /// The receiver is a closed repo type or module and has no such member.
    NoMember,
    /// A coroutine that was not awaited.
    Coroutine,
    /// Awaiting something that is not a known coroutine.
    NotAwaitable,
    NoAnnotation,
    NoneValue,
    /// Union annotations whose members disagree.
    Union,
    /// Attribute sources that evaluate to different types.
    Disagree,
    /// No attribute source evaluates to a type.
    NoType,
    Cycle,
    Depth,
    NotCallable,
    NotAType,
    TypeVar,
    /// A module's `__enter__` / `__aenter__` is not declared in the repo.
    NoEnter,
    /// A base class that is not in the table and not external.
    UnresolvedBase,
    /// Overloads or duplicate definitions with different signatures.
    Overloaded,
    AmbiguousName,
}

/// The result of evaluating an expression.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// An instance of a repo class (qualname).
    Instance(String),
    /// A repo class itself.
    Class(String),
    Module(String),
    /// A module-level function or static method (qualname).
    Function(String),
    /// A method looked up on `class`, declared as `func` (possibly in a base).
    BoundMethod {
        class: String,
        func: String,
    },
    /// Outside the repo (stdlib, third party, builtins).
    External,
    /// An un-awaited coroutine producing the inner value.
    Coroutine(Box<Value>),
    /// `super()` inside the class.
    Super(String),
    /// A name several star imports define differently.
    Ambiguous(Vec<String>),
    Unknown(Why),
}

/// How a call was bound; each maps onto an `edges.resolution_kind` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum How {
    /// Defined in the caller's own module or class.
    Exact,
    /// Reached through an import or module attribute.
    Import,
    /// A member of the receiver value's own type.
    ReceiverType,
    /// A member inherited from a base class.
    Inherited,
}

impl How {
    /// The `edges.resolution_kind` column value.
    pub const fn as_str(self) -> &'static str {
        match self {
            How::Exact => "exact",
            How::Import => "import",
            How::ReceiverType => "receiver_type",
            How::Inherited => "inherited",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// `target` is a function, method or class qualname.
    Bound {
        target: String,
        how: How,
    },
    Ambiguous(Vec<String>),
    /// The receiver type is known and closed but has no such member, or the
    /// name exists nowhere.
    NoCandidates,
    /// Proven to be outside the repo.
    External,
    Unsupported(Why),
}

// ---------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------

type FuncRef = (usize, usize);

#[derive(Default)]
pub struct PyTypeTable {
    files: Vec<PyFileDecls>,
    modules: HashMap<String, usize>,
    /// Proper prefixes of module names that are not modules themselves
    /// (namespace packages).
    namespaces: BTreeSet<String>,
    classes: HashMap<String, (usize, usize)>,
    funcs: HashMap<String, Vec<FuncRef>>,
    /// `files.id` of each file (parallel to `files`); empty when the table
    /// was built from bare decls.
    file_ids: Vec<i64>,
    paths: HashMap<String, usize>,
}

/// The declaration a call's bound qualname stands for: which file and line
/// the symbol to bind is at.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TargetDecl {
    /// `files.id` of the declaring file (`None` for a table built from bare
    /// decls).
    pub file_id: Option<i64>,
    /// `start_line` of the definition a call runs: the last non-`@overload`
    /// definition, else the last one.
    pub line: i64,
    /// A class rather than a function or method.
    pub is_class: bool,
}

impl PyTypeTable {
    pub fn build(files: Vec<PyFileDecls>) -> Self {
        let mut t = PyTypeTable::default();
        for (fi, f) in files.iter().enumerate() {
            t.modules.entry(f.module.clone()).or_insert(fi);
            for (ci, c) in f.classes.iter().enumerate() {
                t.classes.entry(c.qualname.clone()).or_insert((fi, ci));
            }
            for (ui, u) in f.functions.iter().enumerate() {
                t.funcs
                    .entry(u.qualname.clone())
                    .or_default()
                    .push((fi, ui));
            }
        }
        let names: Vec<String> = t.modules.keys().cloned().collect();
        for m in names {
            let mut prefix = String::new();
            let parts: Vec<&str> = m.split('.').collect();
            for p in &parts[..parts.len().saturating_sub(1)] {
                if !prefix.is_empty() {
                    prefix.push('.');
                }
                prefix.push_str(p);
                if !t.modules.contains_key(&prefix) {
                    t.namespaces.insert(prefix.clone());
                }
            }
        }
        t.files = files;
        t.absolutize_sibling_imports();
        t
    }

    /// An absolute `import util` / `from util import f` in a script run from
    /// its own directory (`sys.path[0]`, a directory without an `__init__`)
    /// names the sibling module: when no top-level module is called `util`
    /// but the importing file's directory has one, the import means that
    /// one. Rewrites those imports' targets.
    fn absolutize_sibling_imports(&mut self) {
        type Rewrites = Vec<(usize, String)>;
        let mut fixes: Vec<(usize, Rewrites, Rewrites)> = Vec::new();
        for (fi, file) in self.files.iter().enumerate() {
            let package = if file.is_package {
                continue;
            } else {
                match file.module.rsplit_once('.') {
                    Some((p, _)) => p,
                    None => continue,
                }
            };
            // A regular package (one with an `__init__`) is imported as a
            // package, where a bare `import util` is the top-level `util`.
            if self.modules.contains_key(package) {
                continue;
            }
            let rewrite = |target: &str| -> Option<String> {
                let first = target.split('.').next()?;
                if self.is_module(first) {
                    return None;
                }
                let sibling = format!("{package}.{first}");
                self.is_module(&sibling)
                    .then(|| format!("{package}.{target}"))
            };
            let imports: Vec<(usize, String)> = file
                .imports
                .iter()
                .enumerate()
                .filter_map(|(i, imp)| rewrite(&imp.target).map(|t| (i, t)))
                .collect();
            let stars: Vec<(usize, String)> = file
                .star_imports
                .iter()
                .enumerate()
                .filter_map(|(i, st)| rewrite(st).map(|t| (i, t)))
                .collect();
            if !imports.is_empty() || !stars.is_empty() {
                fixes.push((fi, imports, stars));
            }
        }
        for (fi, imports, stars) in fixes {
            for (i, target) in imports {
                self.files[fi].imports[i].target = target;
            }
            for (i, target) in stars {
                self.files[fi].star_imports[i] = target;
            }
        }
    }

    pub fn from_loaded(files: &[PyLoadedFile]) -> Self {
        let mut t = Self::build(files.iter().map(|f| f.decls.clone()).collect());
        t.file_ids = files.iter().map(|f| f.file_id).collect();
        t.paths = files
            .iter()
            .enumerate()
            .map(|(i, f)| (f.path.clone(), i))
            .collect();
        t
    }

    /// The module name of the file at `path` (a loaded table only).
    pub fn module_of_path(&self, path: &str) -> Option<&str> {
        let i = *self.paths.get(path)?;
        Some(self.files[i].module.as_str())
    }

    /// Which definition binding `qualname` means. `None` when it names no
    /// declaration, or names both a class and a function (a conditional
    /// `class g` / `def g`: which one runs is unknowable).
    pub fn target_decl(&self, qualname: &str) -> Option<TargetDecl> {
        let file_id = |fi: usize| self.file_ids.get(fi).copied();
        let class = self.classes.get(qualname);
        let funcs = self.funcs.get(qualname);
        match (class, funcs) {
            (Some(_), Some(f)) if !f.is_empty() => None,
            (Some(&(fi, ci)), _) => Some(TargetDecl {
                file_id: file_id(fi),
                line: self.files[fi].classes[ci].start_line,
                is_class: true,
            }),
            (None, Some(f)) => {
                let last = f
                    .iter()
                    .rev()
                    .find(|(fi, ui)| !self.files[*fi].functions[*ui].is_overload)
                    .or_else(|| f.last())?;
                Some(TargetDecl {
                    file_id: file_id(last.0),
                    line: self.files[last.0].functions[last.1].start_line,
                    is_class: false,
                })
            }
            (None, None) => None,
        }
    }

    pub fn is_module(&self, name: &str) -> bool {
        self.modules.contains_key(name) || self.namespaces.contains(name)
    }

    pub fn class_decl(&self, qualname: &str) -> Option<(&PyFileDecls, &PyClassDecl)> {
        let (f, c) = *self.classes.get(qualname)?;
        Some((&self.files[f], &self.files[f].classes[c]))
    }

    fn file(&self, module: &str) -> Option<&PyFileDecls> {
        self.modules.get(module).map(|i| &self.files[*i])
    }

    fn funcs_named(&self, qualname: &str) -> Vec<(&PyFileDecls, &PyFuncDecl)> {
        self.funcs
            .get(qualname)
            .map(|v| {
                v.iter()
                    .map(|(f, u)| (&self.files[*f], &self.files[*f].functions[*u]))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The declaration a call to `qualname` runs: the non-`@overload`
    /// implementation when there is one. `None` when there are none, or the
    /// candidates disagree on the signature.
    fn pick(&self, qualname: &str) -> Option<(&PyFileDecls, &PyFuncDecl)> {
        let all = self.funcs_named(qualname);
        let impls: Vec<_> = all
            .iter()
            .filter(|(_, f)| !f.is_overload)
            .cloned()
            .collect();
        let cands = if impls.is_empty() { all } else { impls };
        let first = *cands.first()?;
        let same = cands.iter().all(|(_, f)| {
            f.returns == first.1.returns && f.is_async == first.1.is_async && f.kind == first.1.kind
        });
        same.then_some(first)
    }
}

// ---------------------------------------------------------------------------
// Builtins
// ---------------------------------------------------------------------------

const BUILTINS: &[&str] = &[
    "abs",
    "all",
    "any",
    "ascii",
    "bin",
    "bool",
    "breakpoint",
    "bytearray",
    "bytes",
    "callable",
    "chr",
    "classmethod",
    "compile",
    "complex",
    "delattr",
    "dict",
    "dir",
    "divmod",
    "enumerate",
    "eval",
    "exec",
    "filter",
    "float",
    "format",
    "frozenset",
    "getattr",
    "globals",
    "hasattr",
    "hash",
    "help",
    "hex",
    "id",
    "input",
    "int",
    "isinstance",
    "issubclass",
    "iter",
    "len",
    "list",
    "locals",
    "map",
    "max",
    "memoryview",
    "min",
    "next",
    "object",
    "oct",
    "open",
    "ord",
    "pow",
    "print",
    "property",
    "range",
    "repr",
    "reversed",
    "round",
    "set",
    "setattr",
    "slice",
    "sorted",
    "staticmethod",
    "str",
    "sum",
    "super",
    "tuple",
    "type",
    "vars",
    "zip",
    "__import__",
    "Exception",
    "BaseException",
    "ValueError",
    "TypeError",
    "KeyError",
    "IndexError",
    "AttributeError",
    "RuntimeError",
    "OSError",
    "IOError",
    "StopIteration",
    "NotImplementedError",
    "ImportError",
    "LookupError",
    "AssertionError",
    "NotImplemented",
    "Ellipsis",
    "True",
    "False",
    "None",
    "ArithmeticError",
    "TimeoutError",
    "ConnectionError",
    "FileNotFoundError",
    "PermissionError",
    "UnicodeError",
    "StopAsyncIteration",
    "ExceptionGroup",
    "BaseExceptionGroup",
    "ZeroDivisionError",
    "OverflowError",
];

// ---------------------------------------------------------------------------
// Evaluator
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
enum Guard {
    Member(String, String),
    Attr(String, String),
    Var(String, String),
}

/// A linearized class hierarchy.
struct Mro {
    classes: Vec<String>,
    /// Some base is outside the repo.
    external: bool,
    /// Some base could not be resolved, or the hierarchy is cyclic.
    unknown: bool,
}

/// The receiver a method was reached through, for `Self` in its annotations.
struct Recv {
    class: String,
}

/// The linearized hierarchies one evaluator computed, kept across
/// evaluators over the same table so a resolve pass pays for each class
/// once (`PyEval::with_cache` / `PyEval::into_cache`).
#[derive(Default)]
pub struct PyMroCache(HashMap<String, Rc<Mro>>);

pub struct PyEval<'a> {
    t: &'a PyTypeTable,
    guard: Vec<Guard>,
    depth: usize,
    member_depth: usize,
    mro_cache: HashMap<String, Rc<Mro>>,
}

/// Evaluate `e` in the scope of `module`.
pub fn type_of(t: &PyTypeTable, e: &PyExpr, module: &str) -> Value {
    PyEval::new(t).type_of(e, module)
}

/// Bind one call site made from `caller_module`.
pub fn resolve_site(t: &PyTypeTable, site: &PyCallSite, caller_module: &str) -> Outcome {
    PyEval::new(t).resolve_site(site, caller_module)
}

fn all_same(values: Vec<Value>) -> Option<Value> {
    let first = values.first()?.clone();
    values.iter().all(|v| *v == first).then_some(first)
}

fn split_last(target: &str) -> Option<(&str, &str)> {
    target.rsplit_once('.')
}

fn is_typevar_call(e: &PyExpr) -> bool {
    let PyExpr::Call(inner) = e else {
        return false;
    };
    let last = match &**inner {
        PyExpr::Name(n) => n.as_str(),
        PyExpr::Attr(_, n) => n.as_str(),
        _ => return false,
    };
    matches!(last, "TypeVar" | "ParamSpec" | "TypeVarTuple" | "NewType")
}

impl<'a> PyEval<'a> {
    pub fn new(t: &'a PyTypeTable) -> Self {
        Self::with_cache(t, PyMroCache::default())
    }

    /// An evaluator that starts from `cache` (built over the same `t`).
    pub fn with_cache(t: &'a PyTypeTable, cache: PyMroCache) -> Self {
        PyEval {
            t,
            guard: Vec::new(),
            depth: 0,
            member_depth: 0,
            mro_cache: cache.0,
        }
    }

    /// The hierarchies computed so far.
    pub fn into_cache(self) -> PyMroCache {
        PyMroCache(self.mro_cache)
    }

    // -- expressions --------------------------------------------------------

    pub fn type_of(&mut self, e: &PyExpr, module: &str) -> Value {
        if self.depth >= MAX_EVAL_DEPTH {
            return Value::Unknown(Why::Depth);
        }
        self.depth += 1;
        let v = self.eval(e, module);
        self.depth -= 1;
        v
    }

    fn eval(&mut self, e: &PyExpr, module: &str) -> Value {
        match e {
            PyExpr::Name(n) => self.lookup_name(module, n),
            PyExpr::Imported(target) => self.imported(target),
            PyExpr::Agree(values) => {
                let vals: Vec<Value> = values.iter().map(|v| self.type_of(v, module)).collect();
                match all_same(vals) {
                    Some(Value::Unknown(w)) => Value::Unknown(w),
                    Some(v) => v,
                    None => Value::Unknown(Why::Disagree),
                }
            }
            PyExpr::Declared(tr) => self.ty_value(tr, module, None),
            PyExpr::SelfRef { class, cls } => {
                if self.t.class_decl(class).is_none() {
                    Value::Unknown(Why::UnresolvedName)
                } else if *cls {
                    Value::Class(class.clone())
                } else {
                    Value::Instance(class.clone())
                }
            }
            PyExpr::Attr(inner, n) => {
                let v = self.type_of(inner, module);
                self.attr(&v, n).0
            }
            PyExpr::Call(inner) => {
                let v = self.type_of(inner, module);
                self.call(&v)
            }
            PyExpr::Await(inner) => match self.type_of(inner, module) {
                Value::Coroutine(v) => *v,
                Value::External => Value::External,
                Value::Unknown(w) => Value::Unknown(w),
                _ => Value::Unknown(Why::NotAwaitable),
            },
            PyExpr::Enter { value, is_async } => {
                let v = self.type_of(value, module);
                self.enter(&v, *is_async)
            }
            PyExpr::Super { class } => {
                if self.t.class_decl(class).is_some() {
                    Value::Super(class.clone())
                } else {
                    Value::Unknown(Why::UnresolvedName)
                }
            }
            PyExpr::Unknown(w) => Value::Unknown(Why::Lowered(*w)),
        }
    }

    fn imported(&mut self, target: &str) -> Value {
        if let Some((parent, last)) = split_last(target)
            && self.t.is_module(parent)
        {
            return self
                .module_member(parent, last)
                .unwrap_or(Value::Unknown(Why::NoMember));
        }
        if self.t.is_module(target) {
            Value::Module(target.to_string())
        } else {
            Value::External
        }
    }

    // -- names and modules --------------------------------------------------

    /// A free name in `module`'s scope, builtins included.
    fn lookup_name(&mut self, module: &str, name: &str) -> Value {
        if let Some(v) = self.scope_lookup(module, name) {
            return v;
        }
        if BUILTINS.contains(&name) {
            return Value::External;
        }
        Value::Unknown(Why::UnresolvedName)
    }

    fn defines(&self, module: &str, name: &str) -> bool {
        let Some(f) = self.t.file(module) else {
            return false;
        };
        let qn = format!("{module}.{name}");
        f.classes.iter().any(|c| c.qualname == qn)
            || f.functions
                .iter()
                .any(|u| u.qualname == qn && u.owner.is_none())
            || f.vars.iter().any(|v| v.name == name)
    }

    /// What `module` binds `name` to: its own definitions, imports, then
    /// star imports. `None` when it binds nothing.
    fn scope_lookup(&mut self, module: &str, name: &str) -> Option<Value> {
        let t = self.t;
        let file = t.file(module)?;
        let qn = format!("{module}.{name}");
        if let Some((fi, _)) = t.classes.get(&qn)
            && t.files[*fi].module == module
        {
            return Some(Value::Class(qn));
        }
        if t.funcs_named(&qn)
            .iter()
            .any(|(f, u)| f.module == module && u.owner.is_none())
        {
            return Some(Value::Function(qn));
        }
        if let Some(var) = file.vars.iter().find(|v| v.name == name) {
            return Some(self.var_value(module, var));
        }
        if let Some(imp) = file.imports.iter().find(|i| i.bound == name) {
            return Some(self.import_value(imp));
        }
        self.star_lookup(file, name)
    }

    fn var_value(&mut self, module: &str, var: &PyVarDecl) -> Value {
        let key = Guard::Var(module.to_string(), var.name.clone());
        if self.guard.contains(&key) {
            return Value::Unknown(Why::Cycle);
        }
        self.guard.push(key);
        let v = match (&var.ty, &var.value) {
            (Some(ty), _) => self.ty_value(ty, module, None),
            (None, Some(e)) if is_typevar_call(e) => Value::Unknown(Why::TypeVar),
            (None, Some(e)) => self.type_of(e, module),
            (None, None) => Value::Unknown(Why::Lowered(LowerWhy::Rebound)),
        };
        self.guard.pop();
        v
    }

    fn import_value(&mut self, imp: &PyImport) -> Value {
        if imp.member {
            return self.imported(&imp.target);
        }
        if self.t.is_module(&imp.target) {
            Value::Module(imp.target.clone())
        } else {
            Value::External
        }
    }

    fn exports(&self, module: &str, name: &str) -> bool {
        match self.t.file(module) {
            Some(f) => match &f.all {
                Some(all) => all.iter().any(|a| a == name),
                None => !name.starts_with('_'),
            },
            None => true,
        }
    }

    fn star_lookup(&mut self, file: &PyFileDecls, name: &str) -> Option<Value> {
        let mut found: Vec<Value> = Vec::new();
        for star in &file.star_imports {
            if !self.t.is_module(star) || !self.exports(star, name) {
                continue;
            }
            if let Some(v) = self.module_member(star, name) {
                found.push(v);
            }
        }
        let first = found.first()?.clone();
        if found.iter().all(|v| *v == first) {
            return Some(first);
        }
        let mut names: Vec<String> = found
            .iter()
            .filter_map(|v| match v {
                Value::Class(q) | Value::Function(q) | Value::Module(q) => Some(q.clone()),
                _ => None,
            })
            .collect();
        names.sort();
        names.dedup();
        Some(if names.len() > 1 {
            Value::Ambiguous(names)
        } else {
            Value::Unknown(Why::AmbiguousName)
        })
    }

    /// `module.name` as Python resolves it: a name the module defines or
    /// imports (re-exports followed), then a submodule. A name bound in a
    /// package `__init__` wins over a same-named submodule.
    fn module_member(&mut self, module: &str, name: &str) -> Option<Value> {
        let key = Guard::Member(module.to_string(), name.to_string());
        if self.guard.contains(&key) || self.member_depth >= MAX_MEMBER_DEPTH {
            return self.submodule(module, name);
        }
        self.guard.push(key);
        self.member_depth += 1;
        let v = self
            .scope_lookup(module, name)
            .or_else(|| self.submodule(module, name));
        self.member_depth -= 1;
        self.guard.pop();
        v
    }

    fn submodule(&self, module: &str, name: &str) -> Option<Value> {
        let sub = format!("{module}.{name}");
        self.t.is_module(&sub).then_some(Value::Module(sub))
    }

    // -- classes ------------------------------------------------------------

    fn mro(&mut self, class: &str) -> Rc<Mro> {
        if let Some(m) = self.mro_cache.get(class) {
            return m.clone();
        }
        let mut visiting = Vec::new();
        let m = Rc::new(self.linearize(class, &mut visiting));
        self.mro_cache.insert(class.to_string(), m.clone());
        m
    }

    fn linearize(&mut self, class: &str, visiting: &mut Vec<String>) -> Mro {
        let mut out = Mro {
            classes: vec![class.to_string()],
            external: false,
            unknown: false,
        };
        let t = self.t;
        let Some((file, decl)) = t.class_decl(class) else {
            out.unknown = true;
            return out;
        };
        if visiting.iter().any(|v| v == class) {
            out.classes.clear();
            out.unknown = true;
            return out;
        }
        visiting.push(class.to_string());
        let mut seqs: Vec<Vec<String>> = Vec::new();
        let mut direct: Vec<String> = Vec::new();
        for base in &decl.bases {
            let PyTypeRef::Name { path, .. } = base else {
                out.unknown = true;
                continue;
            };
            if path.len() == 1 && path[0] == "object" {
                continue;
            }
            match self.resolve_path(path, &file.module) {
                Value::Class(b) => {
                    let lin = self.linearize(&b, visiting);
                    out.external |= lin.external;
                    out.unknown |= lin.unknown;
                    if lin.classes.is_empty() {
                        continue;
                    }
                    direct.push(b);
                    seqs.push(lin.classes);
                }
                Value::External => out.external = true,
                _ => out.unknown = true,
            }
        }
        visiting.pop();
        seqs.push(direct.clone());
        let merged = c3_merge(seqs.clone()).unwrap_or_else(|| {
            let mut flat: Vec<String> = Vec::new();
            for s in &seqs[..seqs.len() - 1] {
                for c in s {
                    if !flat.contains(c) {
                        flat.push(c.clone());
                    }
                }
            }
            flat
        });
        out.classes.extend(merged);
        out
    }

    /// Look `name` up on `class` through its MRO. Returns the value and the
    /// class that declares it. `skip_first` starts after `class` (`super()`).
    fn class_member(
        &mut self,
        class: &str,
        name: &str,
        skip_first: bool,
    ) -> (Value, Option<String>) {
        let mro = self.mro(class);
        let t = self.t;
        for (i, k) in mro.classes.iter().enumerate() {
            if skip_first && i == 0 {
                continue;
            }
            let qn = format!("{k}.{name}");
            if let Some((file, f)) = t.pick(&qn) {
                let v = if f.kind == PyFuncKind::Property {
                    let rc = Recv {
                        class: class.to_string(),
                    };
                    self.return_value(f, &file.module, Some(&rc))
                } else {
                    Value::BoundMethod {
                        class: class.to_string(),
                        func: qn,
                    }
                };
                return (v, Some(k.clone()));
            }
            if !t.funcs_named(&qn).is_empty() {
                // Duplicate definitions that disagree: bind the name, the
                // type is unknown.
                return (
                    Value::BoundMethod {
                        class: class.to_string(),
                        func: qn,
                    },
                    Some(k.clone()),
                );
            }
            if t.classes.contains_key(&qn) {
                return (Value::Class(qn), Some(k.clone()));
            }
            if t.class_decl(k)
                .is_some_and(|(_, c)| c.attrs.iter().any(|a| a.name == name))
            {
                let v = self.attr_value(k, name, class);
                return (v, Some(k.clone()));
            }
        }
        let v = if mro.unknown {
            Value::Unknown(Why::UnresolvedBase)
        } else if mro.external {
            Value::External
        } else {
            Value::Unknown(Why::NoMember)
        };
        (v, None)
    }

    /// The type of attribute `name` declared on `class`: every source that
    /// yields a type must agree.
    fn attr_value(&mut self, class: &str, name: &str, recv_class: &str) -> Value {
        let key = Guard::Attr(class.to_string(), name.to_string());
        if self.guard.contains(&key) {
            return Value::Unknown(Why::Cycle);
        }
        let t = self.t;
        let Some((file, decl)) = t.class_decl(class) else {
            return Value::Unknown(Why::NoType);
        };
        let Some(attr) = decl.attrs.iter().find(|a| a.name == name) else {
            return Value::Unknown(Why::NoType);
        };
        self.guard.push(key);
        let rc = Recv {
            class: recv_class.to_string(),
        };
        let mut types: Vec<Value> = Vec::new();
        let mut first_unknown: Option<Why> = None;
        for src in &attr.sources {
            let v = match src {
                PyAttrSource::Declared(tr) | PyAttrSource::Property(tr) => {
                    self.ty_value(tr, &file.module, Some(&rc))
                }
                PyAttrSource::Value(e) => self.type_of(e, &file.module),
            };
            match v {
                Value::Unknown(w) => {
                    first_unknown.get_or_insert(w);
                }
                v => {
                    if !types.contains(&v) {
                        types.push(v);
                    }
                }
            }
        }
        self.guard.pop();
        match types.len() {
            0 => Value::Unknown(first_unknown.unwrap_or(Why::NoType)),
            1 => types.remove(0),
            _ => Value::Unknown(Why::Disagree),
        }
    }

    // -- attributes, calls --------------------------------------------------

    /// `recv.name`, with the class that declares it (for classification).
    fn attr(&mut self, recv: &Value, name: &str) -> (Value, Option<String>) {
        match recv {
            Value::Module(m) => {
                let m = m.clone();
                let v = self.module_member(&m, name).unwrap_or_else(|| {
                    if self.has_external_star(&m) {
                        Value::Unknown(Why::UnresolvedName)
                    } else {
                        Value::Unknown(Why::NoMember)
                    }
                });
                (v, None)
            }
            Value::Instance(c) | Value::Class(c) => {
                let c = c.clone();
                self.class_member(&c, name, false)
            }
            Value::Super(c) => {
                let c = c.clone();
                self.class_member(&c, name, true)
            }
            Value::External => (Value::External, None),
            Value::Coroutine(_) => (Value::Unknown(Why::Coroutine), None),
            Value::Unknown(w) => (Value::Unknown(w.clone()), None),
            _ => (Value::Unknown(Why::NotCallable), None),
        }
    }

    fn has_external_star(&self, module: &str) -> bool {
        self.t
            .file(module)
            .is_some_and(|f| f.star_imports.iter().any(|s| !self.t.is_module(s)))
    }

    fn call(&mut self, callee: &Value) -> Value {
        match callee {
            Value::Class(q) => Value::Instance(q.clone()),
            Value::Function(q) => self.func_return(q, None),
            Value::BoundMethod { class, func } => {
                let rc = Recv {
                    class: class.clone(),
                };
                self.func_return(func, Some(&rc))
            }
            Value::Instance(q) => {
                let q = q.clone();
                match self.class_member(&q, "__call__", false).0 {
                    m @ Value::BoundMethod { .. } => self.call(&m),
                    Value::External => Value::External,
                    _ => Value::Unknown(Why::NotCallable),
                }
            }
            Value::External => Value::External,
            Value::Unknown(w) => Value::Unknown(w.clone()),
            _ => Value::Unknown(Why::NotCallable),
        }
    }

    fn func_return(&mut self, qualname: &str, rc: Option<&Recv>) -> Value {
        let t = self.t;
        match t.pick(qualname) {
            Some((file, f)) => self.return_value(f, &file.module, rc),
            None => Value::Unknown(Why::Overloaded),
        }
    }

    /// What calling `f` produces. `module` is where `f` is declared (its
    /// annotations resolve there); `rc` is the receiver it was reached on.
    fn return_value(&mut self, f: &PyFuncDecl, module: &str, rc: Option<&Recv>) -> Value {
        let Some(ret) = &f.returns else {
            // An unannotated `return self` is `-> Self`.
            return match rc {
                Some(rc) if f.returns_self => {
                    let v = Value::Instance(rc.class.clone());
                    if f.is_async {
                        Value::Coroutine(Box::new(v))
                    } else {
                        v
                    }
                }
                _ => Value::Unknown(Why::NoAnnotation),
            };
        };
        let mut v = None;
        // `def __enter__(self: T) -> T`: the receiver's own type.
        if let (Some(rc), Some(Some(self_ty))) = (rc, f.params.first().map(|p| p.ty.as_ref()))
            && self_ty == ret
            && let PyTypeRef::Name { path, args } = ret
            && path.len() == 1
            && args.is_empty()
            && !matches!(self.lookup_name(module, &path[0]), Value::Class(_))
        {
            v = Some(Value::Instance(rc.class.clone()));
        }
        let v = v.unwrap_or_else(|| self.ty_value(ret, module, rc));
        if f.is_async {
            Value::Coroutine(Box::new(v))
        } else {
            v
        }
    }

    fn enter(&mut self, v: &Value, is_async: bool) -> Value {
        match v {
            Value::External => Value::External,
            Value::Instance(c) => {
                let c = c.clone();
                let name = if is_async { "__aenter__" } else { "__enter__" };
                let (m, _) = self.class_member(&c, name, false);
                if !matches!(m, Value::BoundMethod { .. }) {
                    return Value::Unknown(Why::NoEnter);
                }
                match self.call(&m) {
                    Value::Coroutine(inner) if is_async => *inner,
                    Value::Coroutine(_) => Value::Unknown(Why::Coroutine),
                    other if is_async => match other {
                        Value::External => Value::External,
                        _ => Value::Unknown(Why::NotAwaitable),
                    },
                    other => other,
                }
            }
            Value::Unknown(w) => Value::Unknown(w.clone()),
            _ => Value::Unknown(Why::NoEnter),
        }
    }

    // -- annotations --------------------------------------------------------

    /// `a.b.c` resolved from `module`'s scope.
    fn resolve_path(&mut self, path: &[String], module: &str) -> Value {
        let Some(first) = path.first() else {
            return Value::Unknown(Why::NotAType);
        };
        let mut v = self.lookup_name(module, first);
        for seg in &path[1..] {
            v = self.attr(&v, seg).0;
        }
        v
    }

    /// The value an annotation describes: an instance of the named class.
    fn ty_value(&mut self, tr: &PyTypeRef, module: &str, rc: Option<&Recv>) -> Value {
        match tr {
            PyTypeRef::None => Value::Unknown(Why::NoneValue),
            PyTypeRef::Unknown => Value::Unknown(Why::NoAnnotation),
            PyTypeRef::Union(members) => {
                let vals: Vec<Value> = members
                    .iter()
                    .map(|m| self.ty_value(m, module, rc))
                    .collect();
                all_same(vals)
                    .filter(|v| !matches!(v, Value::Unknown(_)))
                    .unwrap_or(Value::Unknown(Why::Union))
            }
            PyTypeRef::Name { path, args } => {
                let last = path.last().map(String::as_str).unwrap_or("");
                if last == "Self" && path.len() <= 2 {
                    return match rc {
                        Some(rc) => Value::Instance(rc.class.clone()),
                        None => Value::Unknown(Why::NotAType),
                    };
                }
                match self.resolve_path(path, module) {
                    Value::Class(q) => Value::Instance(q),
                    Value::External => match (last, args.as_slice()) {
                        ("Type" | "type", [a]) => match self.ty_value(a, module, rc) {
                            Value::Instance(q) => Value::Class(q),
                            Value::External => Value::External,
                            _ => Value::Unknown(Why::NotAType),
                        },
                        ("Awaitable", [a]) => {
                            Value::Coroutine(Box::new(self.ty_value(a, module, rc)))
                        }
                        ("Coroutine", [_, _, a]) => {
                            Value::Coroutine(Box::new(self.ty_value(a, module, rc)))
                        }
                        _ => Value::External,
                    },
                    Value::Unknown(w) => Value::Unknown(w),
                    _ => Value::Unknown(Why::NotAType),
                }
            }
        }
    }

    // -- call sites ---------------------------------------------------------

    pub fn resolve_site(&mut self, site: &PyCallSite, module: &str) -> Outcome {
        let (v, how) = match &site.callee {
            PyExpr::Name(n) => {
                let how = if self.defines(module, n) {
                    How::Exact
                } else {
                    How::Import
                };
                (self.lookup_name(module, n), how)
            }
            PyExpr::Attr(recv, n) => {
                let rv = self.type_of(recv, module);
                let (v, found_in) = self.attr(&rv, n);
                let how = match (&rv, &found_in) {
                    (Value::Module(_), _) => How::Import,
                    (Value::Super(_), _) => How::Inherited,
                    (Value::Instance(c) | Value::Class(c), Some(k)) if c == k => {
                        if matches!(**recv, PyExpr::SelfRef { .. }) {
                            How::Exact
                        } else {
                            How::ReceiverType
                        }
                    }
                    (Value::Instance(_) | Value::Class(_), Some(_)) => How::Inherited,
                    _ => How::ReceiverType,
                };
                (v, how)
            }
            other => {
                let how = if matches!(other, PyExpr::Imported(_)) {
                    How::Import
                } else {
                    How::ReceiverType
                };
                (self.type_of(other, module), how)
            }
        };
        match v {
            Value::Class(q) | Value::Function(q) | Value::BoundMethod { func: q, .. } => {
                Outcome::Bound { target: q, how }
            }
            Value::External => Outcome::External,
            Value::Ambiguous(names) => Outcome::Ambiguous(names),
            Value::Unknown(Why::NoMember | Why::UnresolvedName) => Outcome::NoCandidates,
            Value::Unknown(w) => Outcome::Unsupported(w),
            _ => Outcome::Unsupported(Why::NotCallable),
        }
    }

    /// Debug aid: the value of every sub-expression along the callee chain.
    pub fn trace_site(&mut self, site: &PyCallSite, module: &str) -> String {
        let mut out = String::new();
        let mut cur = Some(&site.callee);
        let mut depth = 0;
        while let Some(e) = cur {
            let text = format!("{e:?}");
            let end = (0..=text.len().min(110))
                .rev()
                .find(|i| text.is_char_boundary(*i))
                .unwrap_or(0);
            out.push_str(&format!(
                "{}{} => {:?}\n",
                " ".repeat(depth * 2),
                &text[..end],
                self.type_of(e, module)
            ));
            cur = match e {
                PyExpr::Attr(i, _) | PyExpr::Call(i) | PyExpr::Await(i) => Some(&**i),
                PyExpr::Enter { value, .. } => Some(&**value),
                _ => None,
            };
            depth += 1;
        }
        out.push_str(&format!("=> {:?}\n", self.resolve_site(site, module)));
        out
    }
}

/// C3 merge of linearizations; `None` when inconsistent.
fn c3_merge(mut seqs: Vec<Vec<String>>) -> Option<Vec<String>> {
    let mut out = Vec::new();
    loop {
        seqs.retain(|s| !s.is_empty());
        if seqs.is_empty() {
            return Some(out);
        }
        let cand = seqs
            .iter()
            .map(|s| &s[0])
            .find(|c| !seqs.iter().any(|s| s[1..].contains(c)))?
            .clone();
        for s in seqs.iter_mut() {
            if s[0] == cand {
                s.remove(0);
            }
        }
        out.push(cand);
    }
}

/// Convenience for tests and tooling: the trace of one site.
pub fn trace_site(t: &PyTypeTable, site: &PyCallSite, module: &str) -> String {
    PyEval::new(t).trace_site(site, module)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::resolver::ALL_RESOLUTION_KINDS;

    #[test]
    fn how_maps_onto_existing_resolution_kinds() {
        for h in [How::Exact, How::Import, How::ReceiverType, How::Inherited] {
            assert!(ALL_RESOLUTION_KINDS.contains(&h.as_str()), "{h:?}");
        }
    }

    #[test]
    fn c3_merges_diamonds_and_rejects_conflicts() {
        let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        let m = c3_merge(vec![s(&["B", "A"]), s(&["C", "A"]), s(&["B", "C"])]).unwrap();
        assert_eq!(m, s(&["B", "C", "A"]));
        assert!(c3_merge(vec![s(&["A", "B"]), s(&["B", "A"])]).is_none());
    }
}
