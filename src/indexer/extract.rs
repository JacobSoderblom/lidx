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
    /// signature the extractor can't see (another file). Holds the encoded
    /// column text (`ReceiverType::deferred_return`); the resolver swaps it
    /// for `Known(return type)` -- or `Unresolved` -- once every symbol
    /// exists. Persisted as-is so a later retry re-resolves it.
    Deferred(String),
}

/// A parsed `ReceiverType::Deferred` marker.
pub struct DeferredReturn<'a> {
    pub awaited: bool,
    pub static_only: bool,
    pub type_name: &'a str,
    pub method: &'a str,
}

/// Column-text prefix of `ReceiverType::Deferred`. `@` can't start a type name.
pub const DEFERRED_RETURN_PREFIX: &str = "@ret:";

/// Column-text prefix of a Rust deferred receiver (`@ret:` family, so the
/// resolver's `LIKE '@ret:%'` retry scans cover it).
pub const RUST_DEFERRED_PREFIX: &str = "@ret:r:";

/// Where a Rust deferred receiver's declared type is read from.
#[derive(Clone, Debug, PartialEq)]
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
#[derive(Clone, Debug, PartialEq)]
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
#[derive(Clone, Debug, PartialEq)]
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
    /// `Deferred` for "the (optionally awaited) return value of
    /// `type_name.method`".
    ///
    /// `static_only` marks a receiver spelled like a bare type name
    /// (`Type.Method()`): only a `static` method can be called that way.
    pub fn deferred_return(
        type_name: &str,
        method: &str,
        awaited: bool,
        static_only: bool,
    ) -> Self {
        Self::Deferred(format!(
            "{DEFERRED_RETURN_PREFIX}{}{}:{type_name}.{method}",
            if awaited { "a" } else { "" },
            if static_only { "s" } else { "" },
        ))
    }

    /// Inverse of `deferred_return` on column text.
    pub fn parse_deferred_return(column: &str) -> Option<DeferredReturn<'_>> {
        let rest = column.strip_prefix(DEFERRED_RETURN_PREFIX)?;
        let (flags, callee) = rest.split_once(':')?;
        let (ty, method) = callee.rsplit_once('.')?;
        Some(DeferredReturn {
            awaited: flags.contains('a'),
            static_only: flags.contains('s'),
            type_name: ty,
            method,
        })
    }

    /// Encode as the `edges.receiver_type` column value: `None` = not
    /// tracked (legacy resolution tiers apply), `Some("")` = tracked but
    /// unresolved/builtin (must not bind, no lookup attempted at all),
    /// `Some(ty)` = tracked with this inferred type name.
    pub fn as_column(&self) -> Option<&str> {
        match self {
            ReceiverType::NotTracked => None,
            ReceiverType::Unresolved => Some(""),
            ReceiverType::Known(ty) | ReceiverType::Deferred(ty) => Some(ty.as_str()),
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
