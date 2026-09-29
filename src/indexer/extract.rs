use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct SymbolInput {
    pub kind: String,
    pub name: String,
    pub qualname: String,
    pub start_line: i64,
    pub start_col: i64,
    pub end_line: i64,
    pub end_col: i64,
    pub start_byte: i64,
    pub end_byte: i64,
    pub signature: Option<String>,
    pub docstring: Option<String>,
}

/// Inferred type of a method call's receiver (e.g. the `store` in
/// `store.append(x)`), used to gate fuzzy CALLS-edge resolution so a call
/// through a receiver of unknown or builtin type does not bind to an
/// unrelated same-named method elsewhere in the index (see issue #45's
/// measurement: 84% of bound CALLS edges on a real corpus were exactly this
/// — a local variable's `.append`/`.write_line`/... colliding with an
/// unrelated domain method of the same name).
///
/// Populated by the Python extractor for wave 1; the DB resolution path
/// this gates is language-agnostic, so future extractors (TypeScript, C#)
/// can populate it the same way without further DB changes.
///
/// ponytail: this is deliberately not a type checker. See
/// `python::infer_receiver_type` and `python::infer_local_types` for
/// exactly which shapes are inferred and which collapse to `Unresolved`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ReceiverType {
    /// No receiver-type signal for this edge: either it isn't a method
    /// call with an inferable receiver shape (a bare function call), or
    /// the base is a name this extractor doesn't track as local (e.g. a
    /// direct class/module reference like `ClassName.static_method()`).
    /// Resolution falls back to the pre-existing exact/two-segment/
    /// bare-name tiers, unchanged.
    #[default]
    NotTracked,
    /// This edge is a method call through a receiver whose type is a
    /// builtin (list/str/dict/...) or otherwise could not be determined.
    /// Resolution must not bind this edge.
    Unresolved,
    /// The receiver's type was inferred to be this name. Resolution must
    /// require the target method to belong to a matching type.
    Known(String),
    /// A `Known` C# type looked up through this scope (an unqualified
    /// interface name), stored as `receiver_type` + `receiver_scope`.
    Scoped { scope: TypeScope, ty: String },
    /// The receiver is the return value of `Type.Method(..)`, whose
    /// signature the extractor can't see (another file). The resolver swaps
    /// it for `Known(return type)` -- or `Unresolved` -- once every symbol
    /// exists. Persisted as a [`DeferredMarker`] so a later retry
    /// re-resolves it.
    Deferred(DeferredReturn),
    /// A Rust receiver traced to a declaration in another file.
    RustDeferred(RustDeferred),
    /// A target-typed `new(..)` passed as a call argument: constructs the
    /// callee's declared parameter type (never a receiver type itself).
    DeferredArgument(DeferredArgument),
}

/// "The (optionally awaited) return value of `base.method(..)`".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredReturn {
    pub base: DeferredBase,
    pub method: String,
    pub awaited: bool,
    /// The base is spelled like a bare type name (`Type.Method()`): only a
    /// `static` method can be called that way.
    pub static_only: bool,
    /// Set on the marker of a call edge whose own target text is just the
    /// called method's name (`a.B().C()` has no printable receiver): the
    /// resolver binds it through the receiver type only.
    pub name_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeferredBase {
    /// A receiver of this type.
    Type(String),
    /// A receiver that is another call's return value (a chained call, or a
    /// `var` bound from a deferred `var`).
    Call(Box<DeferredReturn>),
}

/// "The parameter at `index` (or named `name`) of the `arg_count`-argument
/// call to `callee`": the type a target-typed `new(..)` argument constructs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredArgument {
    /// Zero-based position of the argument in the call.
    pub index: usize,
    /// The argument's name for a named argument (`x: new()`).
    pub name: Option<String>,
    /// How many arguments the call passes, for overload selection.
    pub arg_count: usize,
    /// `Type.Method` or a qualified method name, as spelled at the call site.
    pub callee: String,
}

/// How many calls may nest in a `DeferredReturn` (`a.B().C()...`, `var b =
/// a.G()`) before extraction or resolution gives up and leaves the receiver
/// untracked.
pub const MAX_DEFERRED_DEPTH: usize = 8;

impl DeferredReturn {
    pub fn on_type(type_name: &str, method: &str, awaited: bool, static_only: bool) -> Self {
        Self {
            base: DeferredBase::Type(type_name.to_string()),
            method: method.to_string(),
            awaited,
            static_only,
            name_only: false,
        }
    }

    pub fn on_call(inner: DeferredReturn, method: &str, awaited: bool) -> Self {
        Self {
            base: DeferredBase::Call(Box::new(inner)),
            method: method.to_string(),
            awaited,
            static_only: false,
            name_only: false,
        }
    }

    /// How many calls this nests (1 for a call on a type).
    pub fn depth(&self) -> usize {
        match &self.base {
            DeferredBase::Type(_) => 1,
            DeferredBase::Call(inner) => 1 + inner.depth(),
        }
    }
}

/// Where a Rust deferred receiver's declared type is read from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeferredSource {
    /// Return type of a callee, one of these absolute qualnames.
    Call { candidates: Vec<String> },
    /// Return type of the method `method` of the type `receiver_type`.
    Method {
        receiver_type: String,
        method: String,
    },
    /// Type of a field of the struct/enum `owner`: `field` is `name`, `0`, or
    /// `Variant::name` / `Variant::0` for an enum variant.
    Field { owner: String, field: String },
}

/// One projection applied to a declared type to reach the receiver's type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Step {
    /// `Some(x)` pattern: `Option<T>` -> `T`.
    OptionSome,
    /// `Ok(x)` pattern: `Result<T, E>` -> `T`.
    ResultOk,
    /// `Err(x)` pattern: `Result<T, E>` -> `E`.
    ResultErr,
    /// Element of a tuple.
    Tuple(usize),
    /// Item of a sequence or iterator.
    Elem,
    /// A std method whose result type follows from the receiver's.
    Method(String),
    /// `.await` of a future.
    Await,
}

/// A Rust receiver type the extractor could only trace to a declaration in
/// another file, plus how to project it. Persisted in
/// `edges.receiver_type` (`encode`/`decode`) and finished by the resolver.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RustDeferred {
    pub source: DeferredSource,
    pub steps: Vec<Step>,
    /// Receiver type to use when no declaration is found (the
    /// constructor-name guess); dropped by any step that changes the type.
    pub fallback: Option<String>,
}

/// A deferred resolution the extractor could not finish in one file: the
/// resolver completes it once every symbol exists and re-judges it whenever
/// its callee changes. Stored in `edges` / `unresolved_references` as
/// `deferred_kind` + `deferred` (JSON payload); [`DeferredMarker::encode`] and
/// [`DeferredMarker::decode`] are the only writers and readers of that format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeferredMarker {
    /// C#: the return value of a call in another file.
    Return(DeferredReturn),
    /// Rust: a receiver traced to a declaration in another file.
    Rust(RustDeferred),
    /// C#: a target-typed `new(..)` passed as a call argument.
    Argument(DeferredArgument),
}

/// `deferred_kind` column values, one per [`DeferredMarker`] variant.
pub const DEFERRED_KIND_RETURN: &str = "return";
pub const DEFERRED_KIND_RUST: &str = "rust";
pub const DEFERRED_KIND_ARGUMENT: &str = "argument";

impl DeferredMarker {
    /// `(deferred_kind, deferred)` column values.
    pub fn encode(&self) -> (&'static str, String) {
        let json = |r: serde_json::Result<String>| r.expect("deferred marker serialises");
        match self {
            Self::Return(m) => (DEFERRED_KIND_RETURN, json(serde_json::to_string(m))),
            Self::Rust(m) => (DEFERRED_KIND_RUST, json(serde_json::to_string(m))),
            Self::Argument(m) => (DEFERRED_KIND_ARGUMENT, json(serde_json::to_string(m))),
        }
    }

    /// Inverse of [`DeferredMarker::encode`]; `None` for an unknown kind or a
    /// payload that does not parse.
    pub fn decode(kind: &str, payload: &str) -> Option<Self> {
        match kind {
            DEFERRED_KIND_RETURN => serde_json::from_str(payload).ok().map(Self::Return),
            DEFERRED_KIND_RUST => serde_json::from_str(payload).ok().map(Self::Rust),
            DEFERRED_KIND_ARGUMENT => serde_json::from_str(payload).ok().map(Self::Argument),
            _ => None,
        }
    }
}

/// The receiver columns of one edge / unresolved reference, as stored.
/// `receiver_type` holds only a plain type name (`""` = tracked but
/// unresolved); a deferred receiver has none until the resolver finishes it.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReceiverColumns {
    pub receiver_type: Option<String>,
    pub receiver_scope: Option<String>,
    pub deferred_kind: Option<&'static str>,
    pub deferred: Option<String>,
}

impl ReceiverType {
    /// `Deferred` for "the (optionally awaited) return value of
    /// `type_name.method`".
    pub fn deferred_return(
        type_name: &str,
        method: &str,
        awaited: bool,
        static_only: bool,
    ) -> Self {
        Self::Deferred(DeferredReturn::on_type(
            type_name,
            method,
            awaited,
            static_only,
        ))
    }

    /// The deferred marker this receiver carries, if any.
    pub fn deferred_marker(&self) -> Option<DeferredMarker> {
        match self {
            ReceiverType::Deferred(m) => Some(DeferredMarker::Return(m.clone())),
            ReceiverType::RustDeferred(m) => Some(DeferredMarker::Rust(m.clone())),
            ReceiverType::DeferredArgument(m) => Some(DeferredMarker::Argument(m.clone())),
            _ => None,
        }
    }

    /// The columns to store: `receiver_type` `None` = not tracked (legacy
    /// resolution tiers apply) or deferred, `Some("")` = tracked but
    /// unresolved/builtin (must not bind, no lookup attempted at all),
    /// `Some(ty)` = tracked with this inferred type name.
    pub fn to_columns(&self) -> ReceiverColumns {
        let mut columns = ReceiverColumns::default();
        match self {
            ReceiverType::NotTracked => {}
            ReceiverType::Unresolved => columns.receiver_type = Some(String::new()),
            ReceiverType::Known(ty) => columns.receiver_type = Some(ty.clone()),
            ReceiverType::Scoped { scope, ty } => {
                columns.receiver_type = Some(ty.clone());
                columns.receiver_scope = scope.encode();
            }
            other => {
                if let Some((kind, payload)) = other.deferred_marker().map(|m| m.encode()) {
                    columns.deferred_kind = Some(kind);
                    columns.deferred = Some(payload);
                }
            }
        }
        columns
    }
}

/// Where an unqualified C# receiver type name is looked up, in C# order:
/// enclosing scopes (nested types, then namespaces) innermost first, then
/// the global namespace, then `using` namespaces. Stored in the
/// `receiver_scope` column as `enclosing,..;usings,..`; [`TypeScope::encode`]
/// and [`TypeScope::decode`] are the only writers and readers of that format.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TypeScope {
    pub enclosing: Vec<String>,
    pub usings: Vec<String>,
}

impl TypeScope {
    /// The `receiver_scope` column value; `None` for an empty scope.
    pub fn encode(&self) -> Option<String> {
        if self.enclosing.is_empty() && self.usings.is_empty() {
            return None;
        }
        Some(format!(
            "{};{}",
            self.enclosing.join(","),
            self.usings.join(",")
        ))
    }

    /// Inverse of [`TypeScope::encode`] (`None` = empty scope).
    pub fn decode(column: Option<&str>) -> TypeScope {
        let Some(column) = column else {
            return TypeScope::default();
        };
        let (enclosing, usings) = column.split_once(';').unwrap_or((column, ""));
        let list = |s: &str| {
            s.split(',')
                .filter(|n| !n.is_empty())
                .map(str::to_string)
                .collect()
        };
        TypeScope {
            enclosing: list(enclosing),
            usings: list(usings),
        }
    }
}

/// A call site's argument shape, so the resolver can pick between
/// same-qualname overloads (C# issue #123) and, for `new T(...)`, between
/// the class and its constructor (issue #124). Persisted in the
/// `call_shape` column as `"<n>"` (a call with `n` arguments) or
/// `"new:<n>"` (an object creation).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallShape {
    pub arg_count: u32,
    pub is_new: bool,
}

impl CallShape {
    /// The `call_shape` column text: `"<n>"` or `"new:<n>"`.
    pub fn encode(self) -> String {
        if self.is_new {
            format!("new:{}", self.arg_count)
        } else {
            self.arg_count.to_string()
        }
    }

    /// Inverse of `encode`; `None` for text that isn't a valid shape.
    pub fn decode(raw: &str) -> Option<Self> {
        let (is_new, count) = match raw.strip_prefix("new:") {
            Some(rest) => (true, rest),
            None => (false, raw),
        };
        Some(Self {
            arg_count: count.parse().ok()?,
            is_new,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct EdgeInput {
    pub kind: String,
    pub source_qualname: Option<String>,
    /// Start byte of the source symbol, for a source whose qualname is
    /// shared by several symbols (C# overloads): the edge's source is then
    /// the symbol at this span, not "whichever has the qualname".
    pub source_start_byte: Option<i64>,
    /// Same for the *target* of a `CONTAINS` edge to an overload.
    pub target_start_byte: Option<i64>,
    pub target_qualname: Option<String>,
    pub detail: Option<String>,
    pub evidence_snippet: Option<String>,
    pub evidence_start_line: Option<i64>,
    pub evidence_end_line: Option<i64>,
    pub confidence: Option<f64>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub event_ts: Option<i64>,
    pub receiver_type: ReceiverType,
    /// Fully-qualified candidate qualnames for this call's target, derived
    /// from the calling file's own import context (C# `using` directives /
    /// aliases / enclosing namespace; Python `from x import Y` / `import
    /// x.y as z`; TS/JS `import { f } from "./m"`; Rust `use` bindings) —
    /// populated for a call whose root identifier import-resolves:
    /// `Type.method()` in C#, bare or dotted calls in Python and TS/JS, a
    /// bare call in Rust (see `csharp::import_qualified_candidates`,
    /// `python::import_qualified_candidates`, `javascript::resolve_imports`,
    /// `rust::import_qualified_candidates`).
    ///
    /// Consumed by `Db::insert_edges`'s import tier
    /// (`db::resolver::Resolver::resolve_import`: exact qualname, then an unambiguous
    /// suffix match), which binds only when exactly one symbol resolves.
    /// On a miss, whether the edge then refuses the fuzzy tiers — an
    /// imported name must not bind a same-named unrelated symbol — or falls
    /// through to them is this language's `db::resolver::LanguageProfile::import_miss`
    /// policy (Rust always falls through; Python only for a repo package,
    /// see `is_repo_python_import`; every other language refuses).
    /// Empty for Go, and for call shapes no extractor recognizes.
    ///
    /// Also persisted (JSON-encoded) whenever non-empty -- to the edge's
    /// own `import_candidates` column when it resolves or is a Bridge Edge
    /// kind, else to the `unresolved_references` store row's column of the
    /// same name (issue #79) -- so `Db::retry_unresolved_references` can
    /// retry this same tier later, e.g. once an incremental reindex's
    /// carry-forward step gives the candidate's target file a
    /// current-version symbol row it didn't have yet at insert time. See
    /// the migration 14 comment in `db::migrations`.
    pub import_candidates: Vec<String>,
    /// True when a `CALLS` edge's call site was a genuinely bare
    /// identifier call (`foo()`) rather than anything receiver-qualified
    /// (`obj.foo()`, `self.foo()`, `Type::method()`). Each extractor's
    /// `handle_call` sets this from the callee expression's own AST shape
    /// (not from `target_qualname`, which is container-qualified either
    /// way — see `db::resolver`'s module doc, issue #75). Defaults to
    /// `false` ("not confirmed bare") for every edge kind that doesn't set
    /// it, which is the conservative choice: the resolver's guarded
    /// name-fallback tier only refuses to bind a `method`-kind candidate
    /// when this is `true` *and* the edge kind is `CALLS`, so leaving it
    /// `false` elsewhere never over-restricts.
    pub bare_call: bool,
    /// Argument count / object-creation marker; see `CallShape`. Only the
    /// C# extractor sets it. `None` = no arity signal (resolve as before).
    pub call_shape: Option<CallShape>,
}

#[derive(Debug, Default)]
pub struct ExtractedFile {
    pub symbols: Vec<SymbolInput>,
    pub edges: Vec<EdgeInput>,
    pub file_metrics: Option<FileMetricsInput>,
    pub symbol_metrics: Vec<SymbolMetricsInput>,
    /// Qualnames of symbols in `symbols` this extractor recorded as
    /// private/module-private (Rust: no `pub`; C#/TS: an explicit
    /// `private` modifier) — see `db::resolver::VisibilityRule::Recorded`.
    /// Applied to the `symbols.visibility` column by `Db::set_private_symbols`
    /// after the symbols themselves are inserted (kept separate from
    /// `SymbolInput` so adding this doesn't touch its ~60 existing call
    /// sites). Empty for languages with no recorded visibility rule
    /// (Python) or a derived one that needs no storage (Go: capitalization).
    pub private_qualnames: Vec<String>,
    /// Qualnames of methods this extractor recorded as `static` (C#: every
    /// same-qualname overload is). Recorded into `symbols.visibility` next
    /// to `private` by `Db::set_private_symbols`; only the C# deferred
    /// `Type.Method()` receiver reads it.
    pub static_qualnames: Vec<String>,
    /// JS/TS only: hash of the file's export surface (see
    /// `javascript::export_surface_hash`), stored so a later sync can tell a
    /// body-only edit from one that changes what importers resolve.
    pub export_surface: Option<i64>,
    /// `(qualname, start_line)` of each member declared `override` (C#),
    /// per overload. Recorded into `symbols.visibility` as `override`;
    /// dispatch only pairs a base-class member with an override.
    pub override_symbols: Vec<(String, i64)>,
}
use crate::metrics::{FileMetricsInput, SymbolMetricsInput};
use anyhow::Result;
use std::path::Path;

pub trait LanguageExtractor {
    fn module_name_from_rel_path(&self, rel_path: &str) -> String;
    fn extract(&mut self, source: &str, module_name: &str) -> Result<ExtractedFile>;
    /// Project-wide directives (C# `global using`) the next `extract` call
    /// applies on top of the file's own; default: none.
    fn set_project_globals(&mut self, _globals: &[String]) {}
    fn resolve_imports(
        &self,
        _repo_root: &Path,
        _file_rel_path: &str,
        _module_name: &str,
        _edges: &mut Vec<EdgeInput>,
    ) {
        // default no-op
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deferred_markers_round_trip() {
        let rust = RustDeferred {
            source: DeferredSource::Field {
                owner: "Slot".into(),
                field: "Full::0".into(),
            },
            steps: vec![
                Step::OptionSome,
                Step::Tuple(12),
                Step::Method("unwrap".into()),
            ],
            fallback: Some("Engine".into()),
        };
        let markers = [
            DeferredMarker::Return(DeferredReturn::on_call(
                DeferredReturn::on_type("Repo", "Create", true, true),
                "Load",
                false,
            )),
            DeferredMarker::Rust(rust),
            DeferredMarker::Argument(DeferredArgument {
                index: 1,
                name: Some("x".into()),
                arg_count: 2,
                callee: "Helper.Make".into(),
            }),
        ];
        for marker in markers {
            let (kind, payload) = marker.encode();
            assert_eq!(DeferredMarker::decode(kind, &payload), Some(marker));
        }
        assert_eq!(DeferredMarker::decode("bogus", "{}"), None);
    }

    #[test]
    fn type_scope_round_trips() {
        let scope = TypeScope {
            enclosing: vec!["A.B.Outer".into(), "A.B".into(), "A".into()],
            usings: vec!["N1".into(), "N2".into()],
        };
        assert_eq!(TypeScope::decode(scope.encode().as_deref()), scope);
        assert_eq!(TypeScope::default().encode(), None);
        let only_usings = TypeScope {
            enclosing: vec![],
            usings: vec!["N1".into()],
        };
        assert_eq!(
            TypeScope::decode(only_usings.encode().as_deref()),
            only_usings
        );
    }
}
