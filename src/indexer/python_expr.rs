//! The expression language Python call resolution evaluates.
//!
//! A [`PyExpr`] is a self-contained, location-free description of how a value
//! is built: names are free (resolved at evaluation against the module a
//! file declares), locals are already inlined by lowering
//! (`python_lower`), and anything lowering cannot follow is an explicit
//! [`PyExpr::Unknown`] carrying the reason. It serializes with compact tags
//! so it can ride in a declaration payload or an edge's deferred payload.

use crate::indexer::python_types::PyTypeRef;
use serde::{Deserialize, Serialize};

/// Deepest expression nesting lowering keeps; deeper becomes `Unknown(Depth)`.
pub const MAX_DEPTH: usize = 8;
/// Nodes one lowered expression may consume (inlined locals included);
/// beyond it the rest becomes `Unknown(Budget)`.
pub const MAX_BUDGET: usize = 64;

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum PyExpr {
    /// A free name, resolved at evaluation against the caller file's module
    /// scope, its imports, star imports, then builtins.
    #[serde(rename = "n")]
    Name(String),
    /// A name bound by a function-local import (`def f(): import a.b as x`),
    /// carrying the absolute dotted target it stands for (`a.b`; for
    /// `from p import q` the target is `p.q`, which may name a module or a
    /// member: evaluation decides). It shadows module scope, so it is not a
    /// free [`PyExpr::Name`].
    #[serde(rename = "i")]
    Imported(String),
    /// An annotated parameter or annotated local.
    #[serde(rename = "d")]
    Declared(PyTypeRef),
    /// `self` (or `cls`) inside a method of `class` (a qualname).
    #[serde(rename = "s")]
    SelfRef {
        #[serde(rename = "c")]
        class: String,
        #[serde(rename = "k", default, skip_serializing_if = "is_false")]
        cls: bool,
    },
    #[serde(rename = "a")]
    Attr(Box<PyExpr>, String),
    /// The result of calling the inner value.
    #[serde(rename = "c")]
    Call(Box<PyExpr>),
    #[serde(rename = "w")]
    Await(Box<PyExpr>),
    /// The `as` target of `with` / `async with`.
    #[serde(rename = "e")]
    Enter {
        #[serde(rename = "v")]
        value: Box<PyExpr>,
        #[serde(rename = "a", default, skip_serializing_if = "is_false")]
        is_async: bool,
    },
    /// `super()` inside `class`.
    #[serde(rename = "u")]
    Super {
        #[serde(rename = "c")]
        class: String,
    },
    /// A name bound by several plain assignments in one scope: evaluates to
    /// a value only when every binding's value is the same.
    #[serde(rename = "g")]
    Agree(Vec<PyExpr>),
    #[serde(rename = "?")]
    Unknown(Why),
}

/// Why lowering gave up on a value.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Why {
    Rebound,
    LoopVar,
    Comprehension,
    Lambda,
    Subscript,
    Literal,
    Builtin,
    Yield,
    Depth,
    Budget,
    Complex,
    Untracked,
    Nonlocal,
    Global,
}

/// One call node, lowered: the callee expression with locals inlined, and
/// the number of arguments (keyword arguments and splats count one each).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct PyCallSite {
    #[serde(rename = "f")]
    pub callee: PyExpr,
    #[serde(rename = "n")]
    pub arg_count: u32,
}
