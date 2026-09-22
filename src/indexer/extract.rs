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
}

impl ReceiverType {
    /// Encode as the `edges.receiver_type` column value: `None` = not
    /// tracked (legacy resolution tiers apply), `Some("")` = tracked but
    /// unresolved/builtin (must not bind, no lookup attempted at all),
    /// `Some(ty)` = tracked with this inferred type name.
    pub fn as_column(&self) -> Option<&str> {
        match self {
            ReceiverType::NotTracked => None,
            ReceiverType::Unresolved => Some(""),
            ReceiverType::Known(ty) => Some(ty.as_str()),
        }
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
    /// x.y as z`) — populated only for a bare `Type.method()`-shaped call
    /// whose receiver is a plain identifier that import-resolves to one or
    /// more namespaces/targets (see `csharp::import_qualified_candidates`,
    /// `python::import_qualified_candidates`).
    ///
    /// Consumed by `Db::insert_edges`'s import-aware exact-match tier
    /// (`db::resolve_import_candidate`), which binds only when exactly one
    /// candidate resolves to a real symbol; 0 or 2+ hits fall through
    /// unchanged to the pre-existing exact/two-segment/bare-name tiers, so
    /// the ambiguity guard is never bypassed, only sometimes avoided by
    /// qualifying an otherwise-ambiguous receiver. Empty for every
    /// extractor that doesn't populate it (TypeScript, Rust, Go, ...) and
    /// for every call shape the populating extractors don't recognize.
    ///
    /// Also persisted (JSON-encoded) to the `edges.import_candidates`
    /// column by `insert_edges` whenever non-empty, so
    /// `Db::resolve_null_target_edges` can retry this same tier later —
    /// e.g. once an incremental reindex's carry-forward step gives the
    /// candidate's target file a current-version symbol row it didn't have
    /// yet at insert time. See the migration 14 comment in
    /// `db::migrations` and the `ponytail:` doc on
    /// `resolve_null_target_edges`.
    pub import_candidates: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ExtractedFile {
    pub symbols: Vec<SymbolInput>,
    pub edges: Vec<EdgeInput>,
    pub file_metrics: Option<FileMetricsInput>,
    pub symbol_metrics: Vec<SymbolMetricsInput>,
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
