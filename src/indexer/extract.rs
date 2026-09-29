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
    /// The receiver is the return value of `Type.Method(..)`, whose
    /// signature the extractor can't see (another file). The resolver swaps
    /// it for `Known(return type)` -- or `Unresolved` -- once every symbol
    /// exists. Persisted as column text (`DeferredReturn::encode`) so a
    /// later retry re-resolves it.
    Deferred(DeferredReturn),
    /// A Rust receiver traced to a declaration in another file.
    RustDeferred(RustDeferred),
    /// A target-typed `new(..)` passed as a call argument: constructs the
    /// callee's declared parameter type (never a receiver type itself).
    DeferredArgument(DeferredArgument),
}

/// "The (optionally awaited) return value of `base.method(..)`".
#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeferredBase {
    /// A receiver of this type.
    Type(String),
    /// A receiver that is another call's return value (a chained call, or a
    /// `var` bound from a deferred `var`).
    Call(Box<DeferredReturn>),
}

/// Column-text prefix of a serialised `DeferredReturn`. `@` can't start a
/// type name.
pub const DEFERRED_RETURN_PREFIX: &str = "@ret:";

/// "The parameter at `index` (or named `name`) of the `arg_count`-argument
/// call to `callee`": the type a target-typed `new(..)` argument constructs.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Every deferred marker (`@ret:`, `@arg:`) starts with this; `@` can't start
/// a type name.
pub const DEFERRED_MARKER_PREFIX: &str = "@";

/// Column-text prefix of a serialised `DeferredArgument`.
pub const DEFERRED_ARG_PREFIX: &str = "@arg:";

impl DeferredArgument {
    /// The `edges.receiver_type` column text.
    pub fn encode(&self) -> String {
        format!(
            "{DEFERRED_ARG_PREFIX}{}:{}:{}:{}",
            self.index,
            self.name.as_deref().unwrap_or(""),
            self.arg_count,
            self.callee
        )
    }

    /// Inverse of `encode`.
    pub fn parse(column: &str) -> Option<Self> {
        let rest = column.strip_prefix(DEFERRED_ARG_PREFIX)?;
        let mut parts = rest.splitn(4, ':');
        let index = parts.next()?.parse().ok()?;
        let name = parts.next().filter(|n| !n.is_empty()).map(str::to_string);
        let arg_count = parts.next()?.parse().ok()?;
        Some(Self {
            index,
            name,
            arg_count,
            callee: parts.next()?.to_string(),
        })
    }
}

/// `edges.call_shape` value on an `RPC_CALL` edge that the resolver derived
/// from a deferred receiver (`Db::rederive_deferred_rpc_calls`).
pub const DERIVED_RPC_SHAPE: &str = "rpc:deferred";

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

    /// The `edges.receiver_type` column text.
    pub fn encode(&self) -> String {
        let base = match &self.base {
            DeferredBase::Type(ty) => ty.clone(),
            DeferredBase::Call(inner) => inner.encode(),
        };
        format!(
            "{DEFERRED_RETURN_PREFIX}{}{}{}:{base}.{}",
            if self.awaited { "a" } else { "" },
            if self.static_only { "s" } else { "" },
            if self.name_only { "n" } else { "" },
            self.method,
        )
    }

    /// Inverse of `encode`.
    pub fn parse(column: &str) -> Option<Self> {
        let rest = column.strip_prefix(DEFERRED_RETURN_PREFIX)?;
        let (flags, callee) = rest.split_once(':')?;
        // Not another language's `@ret:` marker (Rust's is `@ret:r:`).
        if !flags.chars().all(|c| matches!(c, 'a' | 's' | 'n')) {
            return None;
        }
        let (base, method) = callee.rsplit_once('.')?;
        let base = if base.starts_with(DEFERRED_RETURN_PREFIX) {
            DeferredBase::Call(Box::new(Self::parse(base)?))
        } else {
            DeferredBase::Type(base.to_string())
        };
        Some(Self {
            base,
            method: method.to_string(),
            awaited: flags.contains('a'),
            static_only: flags.contains('s'),
            name_only: flags.contains('n'),
        })
    }
}

/// Column-text prefix of a Rust deferred receiver (`@ret:` family, so the
/// resolver's `LIKE '@ret:%'` retry scans cover it).
pub const RUST_DEFERRED_PREFIX: &str = "@ret:r:";

/// Where a Rust deferred receiver's declared type is read from.
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RustDeferred {
    pub source: DeferredSource,
    pub steps: Vec<Step>,
    /// Receiver type to use when no declaration is found (the
    /// constructor-name guess); dropped by any step that changes the type.
    pub fallback: Option<String>,
}

impl Step {
    fn encode(&self) -> String {
        match self {
            Step::OptionSome => "some".into(),
            Step::ResultOk => "ok".into(),
            Step::ResultErr => "err".into(),
            Step::Tuple(i) => format!("t{i}"),
            Step::Elem => "elem".into(),
            Step::Method(m) => format!(".{m}"),
            Step::Await => "await".into(),
        }
    }

    fn decode(text: &str) -> Option<Step> {
        Some(match text {
            "some" => Step::OptionSome,
            "ok" => Step::ResultOk,
            "err" => Step::ResultErr,
            "elem" => Step::Elem,
            "await" => Step::Await,
            t if t.starts_with('t') => Step::Tuple(t[1..].parse().ok()?),
            t => Step::Method(t.strip_prefix('.')?.to_string()),
        })
    }
}

impl RustDeferred {
    /// Column text: `@ret:r:<source>|<x>|<y>|<steps>|<fallback>` with
    /// `call|<candidates ;-joined>|`, `method|<type>|<method>` or
    /// `field|<owner>|<field>` as the source and `,`-joined steps.
    pub fn encode(&self) -> String {
        let (tag, x, y) = match &self.source {
            DeferredSource::Call { candidates } => ("call", candidates.join(";"), String::new()),
            DeferredSource::Method {
                receiver_type,
                method,
            } => ("method", receiver_type.clone(), method.clone()),
            DeferredSource::Field { owner, field } => ("field", owner.clone(), field.clone()),
        };
        let steps: Vec<String> = self.steps.iter().map(Step::encode).collect();
        format!(
            "{RUST_DEFERRED_PREFIX}{tag}|{x}|{y}|{}|{}",
            steps.join(","),
            self.fallback.as_deref().unwrap_or("")
        )
    }

    /// Inverse of `encode`; `None` for any other column text.
    pub fn decode(column: &str) -> Option<RustDeferred> {
        let rest = column.strip_prefix(RUST_DEFERRED_PREFIX)?;
        let mut parts = rest.splitn(5, '|');
        let (tag, x, y) = (parts.next()?, parts.next()?, parts.next()?);
        let source = match tag {
            "call" => DeferredSource::Call {
                candidates: x.split(';').map(str::to_string).collect(),
            },
            "method" => DeferredSource::Method {
                receiver_type: x.to_string(),
                method: y.to_string(),
            },
            "field" => DeferredSource::Field {
                owner: x.to_string(),
                field: y.to_string(),
            },
            _ => return None,
        };
        let steps = parts
            .next()?
            .split(',')
            .filter(|s| !s.is_empty())
            .map(Step::decode)
            .collect::<Option<Vec<_>>>()?;
        let fallback = parts.next()?;
        Some(RustDeferred {
            source,
            steps,
            fallback: (!fallback.is_empty()).then(|| fallback.to_string()),
        })
    }
}

impl ReceiverType {
    /// `DeferredArgument::parse` on column text.
    pub fn parse_deferred_argument(column: &str) -> Option<DeferredArgument> {
        DeferredArgument::parse(column)
    }

    /// Whether an `edges.receiver_type` column value is any deferred marker
    /// (`@ret:` / `@arg:`; `@` can't start a type name), which the resolver
    /// re-judges whenever a callee changes.
    pub fn is_deferred_column(column: &str) -> bool {
        column.starts_with(DEFERRED_MARKER_PREFIX)
    }

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

    /// `DeferredReturn::parse` on column text.
    pub fn parse_deferred_return(column: &str) -> Option<DeferredReturn> {
        DeferredReturn::parse(column)
    }

    /// Encode as the `edges.receiver_type` column value: `None` = not
    /// tracked (legacy resolution tiers apply), `Some("")` = tracked but
    /// unresolved/builtin (must not bind, no lookup attempted at all),
    /// `Some(ty)` = tracked with this inferred type name.
    pub fn as_column(&self) -> Option<std::borrow::Cow<'_, str>> {
        use std::borrow::Cow;
        match self {
            ReceiverType::NotTracked => None,
            ReceiverType::Unresolved => Some(Cow::Borrowed("")),
            ReceiverType::Known(ty) => Some(Cow::Borrowed(ty.as_str())),
            ReceiverType::Deferred(call) => Some(Cow::Owned(call.encode())),
            ReceiverType::RustDeferred(pending) => Some(Cow::Owned(pending.encode())),
            ReceiverType::DeferredArgument(arg) => Some(Cow::Owned(arg.encode())),
        }
    }
}

/// Where an unqualified C# receiver type name is looked up, in C# order:
/// enclosing scopes (nested types, then namespaces) innermost first, then
/// the global namespace, then `using` namespaces. Persisted in front of the
/// type name in the `receiver_type` column as `enclosing,..;usings,..|Type`;
/// [`TypeScope::encode`] and [`TypeScope::decode`] are the only readers and
/// writers of that format.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TypeScope {
    pub enclosing: Vec<String>,
    pub usings: Vec<String>,
}

impl TypeScope {
    /// `ty` with this scope in front (`ty` alone for an empty scope).
    pub fn encode(&self, ty: &str) -> String {
        if self.enclosing.is_empty() && self.usings.is_empty() {
            return ty.to_string();
        }
        format!(
            "{};{}|{ty}",
            self.enclosing.join(","),
            self.usings.join(",")
        )
    }

    /// Inverse of [`TypeScope::encode`]: the scope and the bare type text.
    pub fn decode(column: &str) -> (TypeScope, &str) {
        let Some((scope, ty)) = column.split_once('|') else {
            return (TypeScope::default(), column);
        };
        let (enclosing, usings) = scope.split_once(';').unwrap_or((scope, ""));
        let list = |s: &str| {
            s.split(',')
                .filter(|n| !n.is_empty())
                .map(str::to_string)
                .collect()
        };
        (
            TypeScope {
                enclosing: list(enclosing),
                usings: list(usings),
            },
            ty,
        )
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
mod rust_deferred_tests {
    use super::*;

    #[test]
    fn rust_deferred_round_trips_through_column_text() {
        for source in [
            DeferredSource::Call {
                candidates: vec!["crate::a::f".into(), "crate::b::f".into()],
            },
            DeferredSource::Method {
                receiver_type: "Engine".into(),
                method: "build".into(),
            },
            DeferredSource::Field {
                owner: "Slot".into(),
                field: "Full::0".into(),
            },
        ] {
            for fallback in [None, Some("Engine".to_string())] {
                let deferred = RustDeferred {
                    source: source.clone(),
                    steps: vec![
                        Step::OptionSome,
                        Step::ResultOk,
                        Step::ResultErr,
                        Step::Tuple(12),
                        Step::Elem,
                        Step::Method("unwrap".into()),
                        Step::Await,
                    ],
                    fallback,
                };
                assert_eq!(RustDeferred::decode(&deferred.encode()), Some(deferred));
            }
        }
        assert_eq!(RustDeferred::decode("@ret:s:x"), None);
    }
}

#[cfg(test)]
mod type_scope_tests {
    use super::TypeScope;

    #[test]
    fn type_scope_round_trips_through_the_column_format() {
        let scope = TypeScope {
            enclosing: vec!["A.B.Outer".into(), "A.B".into(), "A".into()],
            usings: vec!["N1".into(), "N2".into()],
        };
        let column = scope.encode("IA<int>");
        assert_eq!(column, "A.B.Outer,A.B,A;N1,N2|IA<int>");
        assert_eq!(TypeScope::decode(&column), (scope, "IA<int>"));
        let empty = TypeScope::default();
        assert_eq!(empty.encode("IA"), "IA");
        assert_eq!(TypeScope::decode("IA"), (empty, "IA"));
        let only_usings = TypeScope {
            enclosing: vec![],
            usings: vec!["N1".into()],
        };
        assert_eq!(
            TypeScope::decode(&only_usings.encode("IA")),
            (only_usings, "IA")
        );
    }
}
