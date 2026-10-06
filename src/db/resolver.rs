//! Edge-target resolution: the one place that decides which symbol an
//! edge points at, and the only producer of `edges.resolution_kind`.
//!
//! `Db::insert_edges` resolves each edge through [`Resolver::resolve`]. The
//! automatic repair passes (`Db::retry_unresolved_references`,
//! `Db::reconcile_unresolved_reference_store`, together `Db::repair_unresolved`)
//! retry the same tiers once more symbols exist, targeted at the
//! `unresolved_references` store (issue #78/#79) rather than a full edge
//! rescan -- the untargeted rescan this module used to also expose
//! (`resolve_null_target_edges`) was retired once the store made it
//! redundant even as an opt-in: since issue #79 stopped writing a NULL-target
//! edge for any kind but a Bridge Edge (see `Db::insert_edges`'s doc), there
//! was nothing left in `edges` for a from-scratch edge rescan to find.
//! Every SQL candidate lookup lives in this module.
//!
//! Issue #77: a symbol keeps its id across a sync as long as its stable_id
//! is unchanged (`differ::compute_symbol_diff` / `Db::update_file_symbols`),
//! so an edge into it never needs relinking; a renamed, moved, deleted, or
//! newly-added target is what falls to the tiers below. Every tier refuses
//! (leaves the target NULL) rather than guessing on ambiguity — see
//! `collapse_exact_candidates`, `build_exact_symbol_map`, and
//! `Db::unbind_edges_for_qualnames` for exactly how each corner of that
//! rule is applied.
//!
//! Tier order, first hit wins:
//! 1. **exact** — `target_qualname` names a symbol verbatim.
//! 2. **import** — one of the extractor's import-qualified candidates names
//!    exactly one symbol (`resolve_import`).
//! 3. **known-external refusal** — the receiver is bound by an import that
//!    didn't resolve here, or is a builtin/unresolved type: stop, bind to
//!    the external stub symbol for this reference instead of the name
//!    tiers (`stub_resolution`, issue #80).
//! 4. **receiver type / inheritance** — a known receiver type's own method,
//!    else the closest ancestor declaring it (`resolve_via_inheritance`).
//! 5. **guarded name fallback** — two trailing segments, then the bare
//!    name. Same language only, and only when exactly one candidate
//!    matches.
//! 6. **known-external fallback** (issue #80) — only once every tier above
//!    has found nothing at all (not ambiguous, not a private refusal): for
//!    a language whose qualified-call syntax never populates
//!    `import_candidates` in the first place (Rust's fully-qualified
//!    `std::`/third-party-crate paths, Go's package-qualified calls), a
//!    last, per-language check (`is_known_external_fallback`) for whether
//!    the reference is unambiguously foreign. Every other language's
//!    known-external signal is already caught by tier 3.
//!
//! Bridge Edge kinds (see `is_bridge_edge_kind`) may cross languages in
//! tiers 4–5 when the same-language lookup misses.
//!
//! ponytail: this is today's order, kept as-is by the #73 refactor. It
//! differs from the #70 spec order in two ways: there is no same
//! scope/module tier yet, and known-external refusal runs before the name
//! tiers rather than last (tier 6 is the one exception -- it has to run
//! last, since it only fires once the name tiers have already found
//! nothing). Language resolution profiles (`LanguageProfile`) now drive
//! the separator and import-miss-fallback per-language checks this tier
//! order used to hardcode; a same scope/module tier and stricter guards
//! are still open (#75).

use super::Db;
use crate::indexer::channel::is_bridge_edge_kind;
use crate::indexer::extract::{
    CallShape, DEFERRED_KIND_ARGUMENT, DEFERRED_KIND_RETURN, DeferredMarker, TypeScope,
};
use crate::model::{has_parameter_list, is_partial_signature};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, Statement, ToSql, named_params, params};
use std::collections::HashMap;
use std::sync::LazyLock;

/// An edge's target as the extractor saw it: what the resolver binds.
pub(crate) struct Reference<'a> {
    /// The call site's target text, e.g. `store.append` or `Db::new`.
    pub target_qualname: Option<&'a str>,
    /// The Edge Kind (`CALLS`, `RPC_CALL`, ...); gates cross-language lookup.
    pub edge_kind: &'a str,
    /// The `edges.receiver_type` column value (see `ReceiverType::to_columns`):
    /// a plain type name, `""` for tracked-but-unresolved, `None` for
    /// untracked or deferred.
    pub receiver_type: Option<&'a str>,
    /// The C# lookup scope of `receiver_type` (`edges.receiver_scope`).
    pub receiver_scope: Option<&'a TypeScope>,
    /// A deferred receiver still to be finished by the language's hook
    /// (`edges.deferred_kind` / `deferred`).
    pub deferred: Option<&'a DeferredMarker>,
    /// Import-qualified guesses for the target (see `EdgeInput::import_candidates`).
    pub import_candidates: &'a [String],
    /// `files.language` of the file the edge was found in.
    pub source_lang: &'a str,
    /// `files.path` of the file the edge was found in — the guarded
    /// name-fallback tier's `VisibilityRule` check (same-file escape
    /// hatch, and Go's package-directory comparison).
    pub source_file_path: &'a str,
    /// The calling symbol's own qualname (`edges.source_symbol_id`'s
    /// qualname, i.e. `EdgeInput::source_qualname` as extracted, not a
    /// resolved id) — Rust's `VisibilityRule::RustModule` compares it
    /// against a private candidate's owning module, since a private item
    /// is visible to that module's descendants too, not just its own
    /// file (issue #75 follow-up, finding D). `None` when the caller
    /// itself didn't resolve to a symbol.
    pub source_qualname: Option<&'a str>,
    /// The calling symbol's own id (`edges.source_symbol_id`). The guarded
    /// name fallback never binds an edge to its own source (issue #244).
    pub source_symbol_id: Option<i64>,
    /// `EdgeInput::bare_call` — true for a genuinely bare identifier call
    /// (`foo()`), gating the guarded name-fallback tier's method-kind
    /// exclusion. Meaningless (ignored) for any `edge_kind` other than
    /// `CALLS`.
    pub bare_call: bool,
    /// `EdgeInput::call_shape` -- arity for overload selection and the
    /// `new T(...)` class-to-constructor refinement (issues #123/#124).
    pub call_shape: Option<CallShape>,
}

/// True for a Python bare call (`foo()`) whose name the caller bound locally
/// (a parameter, assignment or nested `def`), which shadows any module-level
/// symbol, so neither the exact nor the import tier may bind it.
///
/// The Python extractor signals this with `receiver_type == Some("")`
/// ("tracked, but not a repo symbol"). That marker is scoped to Python on
/// purpose: the JS/TS extractor also emits `Some("")` for unshadowed global
/// callables (`Error`, `Map`, `URL`...), and a genuine local `class Error {}`
/// there must still resolve through the exact tier.
fn is_locally_bound_bare_call(r: &Reference<'_>) -> bool {
    r.source_lang == "python"
        && r.edge_kind == "CALLS"
        && r.bare_call
        && r.receiver_type == Some("")
}

/// How a resolved target was found. `as_str` is the `edges.resolution_kind`
/// column value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolutionKind {
    Exact,
    Import,
    /// A Rust path followed through a module's `use` re-export
    /// (`pub use a::b as c`) or across crates, rather than found by its own
    /// qualname (see `Resolver::rust_follow_path`).
    Reexport,
    ReceiverType,
    Inherited,
    TwoSegment,
    BareName,
    /// Issue #80: the reference is known to resolve outside this repo
    /// (an import known not to resolve here, or -- for a language whose
    /// qualified-call syntax never routes through `import_candidates` at
    /// all -- `Resolver::is_known_external_fallback`'s own per-language
    /// check). The target is a stub symbol (`kind = 'external'`,
    /// `resolve_external_stub`), not a real extracted symbol.
    ///
    /// `via_language_fallback` is `true` only for the latter (tier 6) case:
    /// the reference's own text gave no known-external signal at all (no
    /// receiver-type refusal, no unresolved import) -- a per-language
    /// heuristic called it foreign only after every name tier already
    /// missed. `Resolution::stored_receiver_type` keys off this to decide
    /// whether the edge's stored `receiver_type` should force the next
    /// resolution straight back to this tier (tier 3, permanent) or keep
    /// the originally extracted value so a later retry re-runs the name
    /// tiers first (tier 6, since the target may since have been added --
    /// see `Db::retry_external_stub_edges`'s doc).
    External {
        via_language_fallback: bool,
    },
}

impl ResolutionKind {
    /// The `edges.resolution_kind` column value.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Import => "import",
            Self::Reexport => "reexport",
            Self::ReceiverType => "receiver_type",
            Self::Inherited => "inherited",
            Self::TwoSegment => "two_segment",
            Self::BareName => "bare_name",
            Self::External { .. } => "external",
        }
    }
}

/// The guarded name-fallback tier's own resolution kinds (tier 5 -- see the
/// module doc): a heuristic match by name alone, unlike `exact`/`import`/
/// `receiver_type`/`inherited`, which all bind on more than a bare name.
/// Issue #81's default `exclude_resolution_kinds` suggestion set --
/// single source of truth for the `["bare_name", "two_segment"]` literal
/// that used to be duplicated across `rpc/handlers.rs`'s next_hops.
pub(crate) const HEURISTIC_RESOLUTION_KINDS: [&str; 2] = [
    ResolutionKind::BareName.as_str(),
    ResolutionKind::TwoSegment.as_str(),
];

/// Every `edges.resolution_kind` value the resolver can produce -- the
/// single source of truth issue #81's `exclude_resolution_kinds` param
/// validates against (`rpc/handlers.rs`'s `validate_resolution_kinds`), so
/// an unknown or wrong-case kind (`"BARE_NAME"`, `"bogus"`) is rejected
/// with a clear error instead of silently matching nothing.
pub(crate) const ALL_RESOLUTION_KINDS: [&str; 8] = [
    ResolutionKind::Exact.as_str(),
    ResolutionKind::Import.as_str(),
    ResolutionKind::Reexport.as_str(),
    ResolutionKind::ReceiverType.as_str(),
    ResolutionKind::Inherited.as_str(),
    ResolutionKind::TwoSegment.as_str(),
    ResolutionKind::BareName.as_str(),
    ResolutionKind::External {
        via_language_fallback: false,
    }
    .as_str(),
];

/// Why a reference stayed unresolved.
///
/// ponytail: no `CrossLanguageRefused` yet — the guarded name fallback
/// already never crosses languages (`unique_by_pattern`'s same-language
/// round always runs first, and the any-language round is gated by
/// `is_bridge_edge_kind`), so a cross-language miss there just falls
/// through to `NoCandidates`/`Ambiguous` rather than needing its own
/// reason.
///
/// Issue #80 narrowed this variant's scope rather than removing it: a
/// receiver *bound by an import that doesn't resolve uniquely in this
/// index* now resolves (to a stub symbol, `ResolutionKind::External`) --
/// that's issue #80's actual scope, "calls into imports known to resolve
/// outside the repo". A call through a receiver of *builtin/unresolved
/// type* with no import involved (`cells = []` then `cells.append(1)`) is
/// not an import known to resolve elsewhere -- it's a local variable whose
/// type just isn't known -- so it stays `Unresolved(External)` here, same
/// as before #80: one stub per distinct callee name would otherwise be
/// named after whatever local variable happened to call it first,
/// defeating "who calls X?" for every builtin method name. See
/// `Resolver::resolve`'s `refuse_names` branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnresolvedReason {
    /// No tier found any candidate.
    NoCandidates,
    /// Some tier found more than one candidate and refused to pick.
    Ambiguous,
    /// The guarded name fallback (tier 5) found a same-language, same-kind
    /// candidate by name, but every one of them was private/unexported and
    /// in a different file (or, for Go, a different package) than the
    /// reference — see `VisibilityRule`. Only this tier checks visibility;
    /// an exact/import/receiver-type/inherited match never refuses on it.
    Private,
    /// The receiver's type is a builtin or otherwise couldn't be inferred
    /// (`ReceiverType::Unresolved`) and no import binding is involved --
    /// see this type's doc. Distinct from `ResolutionKind::External`, which
    /// is a `Resolved` outcome (issue #80's import-known-external case),
    /// not an `Unresolved` reason.
    External,
    /// A Rust `CALLS` reference resolved to a symbol that is no callable (a
    /// `static`, `const` or field): a lowercase binding passed as a value
    /// (`take(counter)`), not a call. Never stored -- no edge and no
    /// `unresolved_references` row, since retrying would only find it again.
    NotCallable,
}

impl UnresolvedReason {
    /// The `unresolved_references.reason` column value (issue #78).
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NoCandidates => "no_candidates",
            Self::Ambiguous => "ambiguous",
            Self::Private => "private",
            Self::External => "external",
            Self::NotCallable => "not_callable",
        }
    }
}

/// The outcome of resolving one [`Reference`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Resolution {
    Resolved {
        target_id: i64,
        kind: ResolutionKind,
    },
    Unresolved(UnresolvedReason),
}

impl Resolution {
    /// The `edges.target_symbol_id` column value.
    pub(crate) fn target_id(self) -> Option<i64> {
        match self {
            Self::Resolved { target_id, .. } => Some(target_id),
            Self::Unresolved(_) => None,
        }
    }

    /// The `edges.resolution_kind` column value.
    pub(crate) fn kind_column(self) -> Option<&'static str> {
        match self {
            Self::Resolved { kind, .. } => Some(kind.as_str()),
            Self::Unresolved(_) => None,
        }
    }

    /// The `edges.receiver_type` value to persist for a reference whose
    /// extractor reported `extracted`. A known-external resolution reached
    /// via tier 3 (the receiver itself is the known-external signal -- a
    /// refused import or an unresolved/builtin receiver type) is stored as
    /// `""` so that if this edge's stub target is ever cleared (its FK's
    /// `ON DELETE SET NULL` -- see the stub-lifecycle doc on
    /// `Db::prune_orphan_external_symbols`), a later repair pass re-judges
    /// it the same way rather than running the name-based tiers on it.
    ///
    /// A known-external resolution reached via tier 6 instead
    /// (`via_language_fallback`, Rust/Go's per-language fallback -- see
    /// `ResolutionKind::External`'s doc) keeps `extracted` as-is (`None`:
    /// neither language tracks a receiver type at all), so
    /// `Db::retry_external_stub_edges` re-runs the name tiers on it instead
    /// of forcing tier 3's short-circuit -- only a receiver actually known
    /// external should be stored as such.
    pub(crate) fn stored_receiver_type(
        self,
        extracted: Option<&str>,
        deferred: bool,
    ) -> Option<&str> {
        match self {
            // A deferred receiver keeps its marker: it is re-judged whenever
            // its callee changes, so it must never be frozen to `""`.
            Self::Resolved {
                kind:
                    ResolutionKind::External {
                        via_language_fallback: false,
                    },
                ..
            } if !deferred => Some(""),
            _ => extracted,
        }
    }

    /// `Some(reason)` when this outcome is `Unresolved` -- issue #78's
    /// `Db::insert_edges` hook into `unresolved_references`.
    pub(crate) fn unresolved_reason(self) -> Option<UnresolvedReason> {
        match self {
            Self::Resolved { .. } => None,
            Self::Unresolved(reason) => Some(reason),
        }
    }
}

/// A language's qualname conventions, import-miss fallback policy and
/// visibility rule, consulted here instead of a hardcoded per-language
/// check. Registered next to the extractor that needs a non-default one
/// (see `crate::indexer::rust::PROFILE`); every other language falls back
/// to `LanguageProfile::DEFAULT` via `profile_for`. Inheritance lookup is
/// #70's later phase, not this struct.
#[derive(Clone, Copy)]
pub(crate) struct LanguageProfile {
    /// Qualname separator(s) this language's extractor writes, most
    /// specific first. Drives the same-language resolution rounds
    /// (`same_lang_patterns`); the AnyLang Bridge Edge round stays
    /// content-driven (checks every separator, since the target's
    /// language isn't known in advance there).
    pub separators: &'static [&'static str],
    /// Normalize a raw import-binding target (as the extractor's `use`/
    /// `import` parsing found it — still possibly relative, e.g. Rust
    /// `super::x`) to an absolute module qualname, given the qualname of
    /// the module the binding was written in. `Some(rewrite)` only for a
    /// language whose import syntax has such a relative form; `rewrite`
    /// itself returns `None` for a target that needs no change (already
    /// absolute) or can't be rewritten (e.g. `super::` past the crate
    /// root).
    pub normalize_import_target: Option<fn(raw: &str, module: &str) -> Option<String>>,
    /// Whether an import-tier miss (candidates present, none resolved)
    /// refuses the name-based fallback tiers, or falls through to them.
    pub import_miss: ImportMissPolicy,
    /// Whether `resolve_import` retries a missed candidate as a qualname
    /// suffix. Off for languages whose candidates are already absolute
    /// (Rust), where a suffix hit would be a same-named local module.
    pub import_suffix_matching: bool,
    /// Whether `resolve_import`, when no candidate hits, retries each with
    /// its last segment stripped (`mod.x.m` -> `mod.x`), so a member call on
    /// an imported binding whose members aren't indexed (a JS/TS object
    /// literal) binds to the binding itself. Only a `const`/`variable`
    /// parent counts, never a class/module (issue #113).
    pub import_member_fallback: bool,
    /// How the guarded name-fallback tier (`Resolver::same_lang_lookup`,
    /// tier 5 only — see the module doc) decides whether a same-language,
    /// same-kind, cross-file candidate is visible to the reference. Never
    /// consulted by the exact/import/receiver-type/inherited tiers.
    pub visibility: VisibilityRule,
    /// The `RPC_CALL` edges a call site yields when its receiver is a
    /// deferred call returning a generated gRPC client (a factory in another
    /// file); `None` when it is no client. See
    /// `Db::rederive_deferred_rpc_calls`.
    pub deferred_rpc: Option<DeferredRpcFn>,
    /// Whether a call whose receiver type could not be inferred stays
    /// `UnresolvedReason::External` when nothing resolves it. Off for C#,
    /// where an extension method's receiver may be of a type the extractor
    /// cannot name, so the reason must not depend on that.
    pub untyped_receiver_is_external: bool,
    /// Finishes a language's own deferred-receiver marker (a
    /// `DeferredMarker` this language's extractor wrote) from the declarations it
    /// names. `None` for a language without one.
    pub deferred_receiver: Option<ResolveDeferred>,
}

/// A symbol a deferred receiver's declaration query matched.
pub struct Declaration {
    pub qualname: String,
    pub signature: Option<String>,
    pub visibility: Option<String>,
    pub file_id: i64,
}

/// What a `ResolveDeferred` hook may ask the index for. Every query is
/// limited to the hook's language and to live symbols of the current graph.
pub enum DeclarationQuery<'a> {
    /// A function or method with exactly this qualname.
    Callable(&'a str),
    /// A method whose qualname ends `::<qualified>` (`Type::method`).
    Method(&'a str),
    /// A type declaration (struct, enum, class, ...) with this name.
    Type(&'a str),
    /// A trait declaration with this name.
    Trait(&'a str),
    /// Methods named `method` on a type named `ty` (`ty.method`, or
    /// `<namespace>.ty.method`; dot-separated qualnames).
    Member { ty: &'a str, method: &'a str },
}

/// The `using`s and namespaces in scope where a declaration is written.
pub struct ScopeImports {
    pub namespaces: Vec<String>,
    pub aliases: HashMap<String, String>,
}

/// The symbol index as a deferred-receiver hook sees it.
pub trait DeclarationIndex {
    fn declarations(&self, query: DeclarationQuery<'_>) -> Result<Vec<Declaration>>;
    /// Whether a type named `name` is declared in the repo.
    fn is_repo_type(&self, name: &str) -> Result<bool>;
    /// `DeclarationQuery::Member` for `ty`, or -- when it declares none --
    /// for its nearest ancestors (EXTENDS/IMPLEMENTS/INHERITS, breadth
    /// first, like `Resolver::resolve_via_inheritance`) that do. An ancestor
    /// already bound to a symbol is matched by that symbol's full qualname,
    /// one still only named by its text by its trailing name.
    fn inherited_members(&self, ty: &str, method: &str) -> Result<Vec<Declaration>>;
    /// The imports and enclosing namespaces of where `decl` is declared.
    fn imports_in_scope(&self, decl: &Declaration) -> Result<ScopeImports>;
}

/// See `LanguageProfile::deferred_receiver`. `Ok(None)`: not this
/// language's marker. `Ok(Some(None))`: the receiver stays untracked.
/// `Ok(Some(Some(ty)))`: the receiver's type (`""`: known not to bind).
pub type ResolveDeferred =
    fn(marker: &DeferredMarker, index: &dyn DeclarationIndex) -> Result<Option<Option<String>>>;

/// One `RPC_CALL` edge `LanguageProfile::deferred_rpc` yields.
pub struct RpcCallEdge {
    pub target_qualname: String,
    pub detail: String,
}

/// `LanguageProfile::deferred_rpc`: the edges for a call of `method` through
/// the deferred receiver `marker`; `Ok(None)` when it isn't a client.
pub type DeferredRpcFn = fn(
    marker: &DeferredMarker,
    method: &str,
    index: &dyn DeclarationIndex,
) -> Result<Option<Vec<RpcCallEdge>>>;

impl LanguageProfile {
    /// Dot-separated, no relative-import syntax, import-tier miss refuses
    /// the name tiers, suffix matching enabled, no visibility rule — every
    /// language's policy today except Rust, Python, C#, TypeScript/
    /// JavaScript and Go.
    pub(crate) const DEFAULT: LanguageProfile = LanguageProfile {
        separators: &["."],
        normalize_import_target: None,
        import_miss: ImportMissPolicy::Refuse,
        import_suffix_matching: true,
        import_member_fallback: false,
        visibility: VisibilityRule::None,
        deferred_rpc: None,
        untyped_receiver_is_external: true,
        deferred_receiver: None,
    };
}

/// How `Resolver::same_lang_lookup` (the guarded name-fallback tier only)
/// decides whether a cross-file candidate is visible to the reference —
/// see `LanguageProfile::visibility`. A same-file candidate is always
/// visible regardless of this rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VisibilityRule {
    /// No visibility signal is recorded or derivable for this language
    /// (Python: no enforced private/public distinction worth tracking
    /// here). Every cross-file, same-name candidate is visible.
    None,
    /// `symbols.visibility` column: `Some("private")` is refused across
    /// files, anything else (recorded `"public"`... today just `NULL`,
    /// meaning "no modifier recorded") is visible. C# and
    /// TypeScript/JavaScript (`private`) — access is scoped to the class,
    /// not to an enclosing namespace/module, so there is no Rust-style
    /// "visible to a descendant scope" exception to make.
    Recorded,
    /// Rust's `pub`: the `symbols.visibility` check, plus a private
    /// candidate stays visible to its own module *and every descendant
    /// module* (`mod child;` puts a child module in its own file, and
    /// Rust lets it see its parent's private items) — a same-file check
    /// alone under-refuses relative to real `pub`/module-privacy
    /// semantics (issue #75 follow-up, finding D).
    RustModule,
    /// Go: an exported name (its trailing segment starts uppercase) is
    /// visible anywhere; an unexported one is visible only within its own
    /// package — approximated as the same directory prefix the qualname
    /// and file path share (`<dir>/<file-stem>.<name>` / `<dir>/<file-
    /// stem>.go`), so a call from a sibling file in the same package
    /// still resolves (see the golden Go fixture's `siblingUtil` case).
    GoCapitalization,
}

impl VisibilityRule {
    /// Whether a cross-file candidate (already known to be in a
    /// *different* file than the reference — `same_lang_lookup` checks
    /// same-file separately, first) is visible under this rule.
    /// `source_qualname` is the calling symbol's own qualname (`None` if
    /// it didn't resolve to one) — only `RustModule` consults it.
    fn is_visible(
        self,
        candidate_visibility: Option<&str>,
        candidate_qualname: &str,
        candidate_file_path: &str,
        source_file_path: &str,
        source_qualname: Option<&str>,
    ) -> bool {
        match self {
            VisibilityRule::None => true,
            VisibilityRule::Recorded => !is_private(candidate_visibility),
            VisibilityRule::RustModule => {
                !is_private(candidate_visibility)
                    || is_descendant_rust_module(
                        candidate_qualname,
                        source_qualname,
                        candidate_visibility,
                    )
            }
            VisibilityRule::GoCapitalization => {
                let exported = qualname_trailing_name(candidate_qualname)
                    .chars()
                    .next()
                    .is_some_and(|c| c.is_ascii_uppercase());
                exported || package_dir(candidate_file_path) == package_dir(source_file_path)
            }
        }
    }
}

/// `symbols.visibility` is a space-separated modifier list (`private`,
/// `static`); whether it contains `private`.
pub(crate) fn is_private(visibility: Option<&str>) -> bool {
    visibility.is_some_and(|v| v.split_whitespace().any(|m| m == "private"))
}

/// The qualname minus its own trailing name segment — the module, type,
/// etc. it's declared directly inside — or `None` when `qn` has no
/// separator at all (a crate-root item).
fn qualname_container(qn: &str) -> Option<&str> {
    let name_start = last_qualname_separator(qn)?;
    Some(qn[..name_start].trim_end_matches(['.', ':']))
}

/// Rust module-privacy (`VisibilityRule::RustModule`): a private
/// candidate is visible when the caller's own module is the candidate's
/// owning module, or nested inside it (`crate::a` owns a private item
/// that `crate::a::child`, `crate::a::child::grandchild`, ... can all
/// still see) — see issue #75 follow-up, finding D.
///
/// A `pub(super)` / `pub(in path)` candidate records its wider module as a
/// `scope:<module>` modifier in `candidate_visibility`; that module owns it.
fn is_descendant_rust_module(
    candidate_qualname: &str,
    source_qualname: Option<&str>,
    candidate_visibility: Option<&str>,
) -> bool {
    let Some(source_qualname) = source_qualname else {
        return false;
    };
    let scope = candidate_visibility
        .and_then(|v| v.split_whitespace().find_map(|m| m.strip_prefix("scope:")));
    let Some(owner_module) = scope.or_else(|| rust_enclosing_module(candidate_qualname)) else {
        return false;
    };
    let caller_module = rust_enclosing_module(source_qualname).unwrap_or(source_qualname);
    caller_module == owner_module || caller_module.starts_with(&format!("{owner_module}::"))
}

/// The module a Rust item lives in: its qualname minus the trailing name,
/// minus any enclosing type segments (`crate::Error::with_depth` is declared
/// in module `crate`, not in `crate::Error` — an `impl`'s items belong to the
/// module containing the `impl`). Types, traits and enums are `UpperCamel`
/// and modules `snake_case` by Rust convention, which is all the qualname
/// carries.
// ponytail: ceiling -- an UpperCamel segment is taken to be a type and a
// lowercase one a module; a `mod Foo` or a lowercase type (`type t = ..`)
// is misjudged. Upgrade: record each symbol's container kind (or a module
// flag) at extraction and climb by that instead of by spelling.
fn rust_enclosing_module(qn: &str) -> Option<&str> {
    let mut module = qualname_container(qn)?;
    while let Some((head, last)) = module.rsplit_once("::") {
        if last.chars().next().is_some_and(char::is_uppercase) {
            module = head;
        } else {
            break;
        }
    }
    Some(module)
}

/// The directory portion of a `/`-separated path or qualname, up to but
/// not including the last `/` — `""` for a root-level file with no `/` at
/// all. Shared by a Go qualname (`<dir>/<file-stem>.<name>`) and a Go file
/// path (`<dir>/<file-stem>.go`): both share the same prefix up to the
/// file stem, so this one function reads either.
fn package_dir(s: &str) -> &str {
    match s.rfind('/') {
        Some(i) => &s[..i],
        None => "",
    }
}

/// Whether `Resolver::resolve` falls through to the name-based tiers after
/// an import-tier miss — see `LanguageProfile::import_miss`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ImportMissPolicy {
    /// The receiver is known to come from something this index doesn't
    /// (or can't uniquely) resolve; falling through would bind on
    /// name-uniqueness alone (e.g. `datetime.now()` binding to an
    /// unrelated local `FakeClock.now`). Every language's policy except
    /// Rust and Python.
    Refuse,
    /// This language's import targets are already absolute repo
    /// qualnames, so a miss just means the literal target wasn't indexed
    /// (e.g. a `pub use` re-export chain) — not evidence it's external.
    /// Rust's policy.
    FallThrough,
    /// Python's own heuristic (`Resolver::is_repo_python_import`): fall
    /// through only when the import is rooted in a repo package (or is a
    /// relative import). Python's module qualname is its repo-relative
    /// path while the import names the installed package path, so a
    /// failed exact/suffix match is as likely a re-export
    /// (`pkg/__init__.py` re-exporting `X`) as truly external. Needs a DB
    /// lookup, so it stays its own variant rather than folding into
    /// `Refuse`/`FallThrough`.
    PythonRepoHeuristic,
}

/// A Rust call target that is a `::`-qualified path not rooted at `crate::`
/// -- every repo qualname is `crate::`-rooted, so this is a path into `std`/
/// `core`/`alloc`, a third-party crate, or a type outside the repo.
fn is_foreign_rust_path(target_qualname: &str) -> bool {
    target_qualname.contains("::") && !target_qualname.starts_with("crate::")
}

/// Whether `path` lives under a `tests/fixtures/` tree: test data indexed
/// as part of the repo, never a legitimate call target for code outside it.
fn is_fixture_path(path: &str) -> bool {
    path.starts_with("tests/fixtures/")
}

/// Look up a language's resolution profile by its `files.language` value:
/// the extractor-registered `PROFILE` for a language that has one, else
/// `LanguageProfile::DEFAULT`. A language absent from the indexed repo is
/// never looked up — this runs only against the `files.language` of a
/// reference actually being resolved.
///
/// Python's `PythonRepoHeuristic` variant still needs a DB lookup
/// (`Resolver::is_repo_python_import`, below) that only this module can
/// do, but the *choice* of that variant is `python::PROFILE`'s, same as
/// Rust's.
fn profile_for(lang: &str) -> LanguageProfile {
    match lang {
        "rust" => crate::indexer::rust::PROFILE,
        "python" => crate::indexer::python::PROFILE,
        "csharp" => crate::indexer::csharp::PROFILE,
        "javascript" | "typescript" | "tsx" => crate::indexer::javascript::PROFILE,
        "go" => crate::indexer::go::PROFILE,
        _ => LanguageProfile::DEFAULT,
    }
}

/// Same-language fuzzy candidates, for `Resolver::same_lang_lookup`
/// (shared by tiers 4 and 5 — see its doc). Deliberately no `LIMIT`: the
/// method-kind exclusion is a query param (`? = 0 OR s.kind != 'method'`)
/// rather than a post-filter, and `same_lang_lookup` consumes the result
/// lazily with an early exit as soon as a second visible candidate is
/// found — so an unbounded query here doesn't mean unbounded work, but a
/// `LIMIT` would silently truncate the candidate set *before* visibility
/// filtering and corrupt the ambiguity count (issue #75 follow-up, a
/// truncated set could read as "exactly one" when a further, cut-off row
/// was the real unambiguous match, or bind to a decoy instead of refusing
/// as ambiguous). The language `CASE` must agree with
/// `resolution_language_family`.
///
/// The leading `s.name IN (:tail, '.' || :tail)` (the lookup name's trailing
/// segment, bound by `Resolver::unique_by_pattern`/`resolve_type_symbol`) is
/// an index-seekable prefilter (`idx_symbols_name_kind`), not the decision:
/// the `LIKE`s and the case-sensitive re-check below still decide. Without
/// it the leading-wildcard `LIKE`s full-scan `symbols` per lookup (issue
/// #255). It is result-preserving because a symbol's `name` is its
/// qualname's trailing segment, except C# constructors (`.ctor`/`.cctor`,
/// hence the `'.' || :tail` arm) and TS/JS computed or quoted method keys
/// (`[Symbol.iterator]`, `'a.b'`), whose tail ends in `]` or a quote:
/// `name_prefilter_applies` sends those lookups to the unfiltered statement
/// (`NAME_PREFILTER` removed).
///
/// `LIKE` is SQLite's only substring/suffix operator, but it's
/// case-insensitive for ASCII (no `PRAGMA case_sensitive_like` is set) —
/// `s.qualname LIKE '%.Error'` also matches a same-named lowercase
/// `error`. Rewriting these as `GLOB` would fix that, but `GLOB`'s
/// `*`/`?`/`[...]` wildcards would then need escaping for any qualname
/// segment that happens to contain one (e.g. a SQL extractor's bracketed
/// `[Schema].[Table]` identifiers). Simpler and safer: keep `LIKE` as a
/// cheap, case-insensitive pre-filter here, and re-check every row's
/// actual `qualname` case-sensitively in Rust (`matches_name_case_sensitive`)
/// before counting it as a candidate — issue #110.
/// The prefilter clause, removed to derive the unfiltered fallback statements.
const NAME_PREFILTER: &str = "s.name IN (:tail, '.' || :tail)\n       AND ";

const SAME_LANG_SQL: &str = "SELECT s.id, s.visibility, s.qualname, f.path, s.kind, s.signature
     FROM symbols s
     JOIN files f ON s.file_id = f.id
     WHERE s.name IN (:tail, '.' || :tail)
       AND (s.qualname = :name OR s.qualname LIKE :p1 OR s.qualname LIKE :p2)
       AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
       AND (:exclude_method = 0 OR s.kind != 'method')
       AND s.graph_version = :gv
       AND (f.deleted_version IS NULL OR f.deleted_version > :gv)
       AND (CASE WHEN f.language IN ('typescript', 'tsx') THEN 'javascript' ELSE f.language END) = :lang
       -- Issue #181: a C# explicit interface impl `C.IA.Run` has no parent
       -- symbol `C.IA`; it is only reachable through the interface, so it is
       -- never a name-fallback candidate (it would make `IA.Run` ambiguous).
       -- Kept inline (not in LanguageProfile): a SQL-side existence check.
       AND (f.language != 'csharp' OR s.kind != 'method' OR EXISTS (
            SELECT 1 FROM symbols p
             WHERE p.graph_version = s.graph_version
               AND p.qualname = substr(s.qualname, 1, length(s.qualname) - length(s.name) - 1)))";

/// Cross-language fuzzy candidates, for Bridge Edge kinds only. Same
/// case-insensitive-`LIKE` caveat as `SAME_LANG_SQL` (issue #110): selects
/// `s.qualname` too, so `Resolver::any_lang_lookup` can re-check each row
/// case-sensitively. Deliberately no `LIMIT`, same reasoning as
/// `SAME_LANG_SQL`'s doc — a case-sensitive re-check after the fetch means
/// a `LIMIT` could truncate the set before a real match past the cutoff is
/// ever seen.
const ANY_LANG_SQL: &str = "SELECT s.id, s.qualname, s.kind, s.signature, f.language
     FROM symbols s
     JOIN files f ON s.file_id = f.id
     WHERE s.name IN (:tail, '.' || :tail)
       AND (s.qualname = :name OR s.qualname LIKE :p1 OR s.qualname LIKE :p2)
       AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
       AND s.graph_version = :gv
       AND (f.deleted_version IS NULL OR f.deleted_version > :gv)";

/// A type's recorded EXTENDS/IMPLEMENTS/INHERITS edges, for
/// `resolve_via_inheritance` -- unioned across both shapes issue #79 leaves
/// a base-class reference in: a resolved (or still-ambiguous-but-written,
/// pre-#79-holdover) edge in `edges`, and a pending, self-contained
/// `unresolved_references` row for one that never resolved to a symbol
/// (EXTENDS/IMPLEMENTS/INHERITS isn't a Bridge Edge kind, so `insert_edges`
/// writes no edge at all for those -- see its doc). The hierarchy walk only
/// ever needs the ancestor's *qualname text* (to look up its own methods by
/// name pattern), never its resolved id, so a pending row's `reference_name`
/// serves exactly as well as a resolved edge's `target_qualname`.
/// `edges.id`/`unresolved_references.id` is each table's own insertion
/// order, which mirrors source order (each extractor emits a class's
/// base-list edges in one pass, in the order the bases are written) --
/// `resolve_via_inheritance` doesn't break ties on it, so interleaving the
/// two tables' own orders via `UNION ALL` (rather than a single merged
/// order) doesn't change its result, only harmless to iterate.
const HIERARCHY_SQL: &str = "SELECT target_symbol_id, target_qualname
     FROM edges
     WHERE source_symbol_id = ?1
       AND kind IN ('EXTENDS', 'IMPLEMENTS', 'INHERITS')
       AND graph_version = ?2
       AND target_qualname IS NOT NULL

     UNION ALL

     SELECT NULL, reference_name
     FROM unresolved_references
     WHERE source_symbol_id = ?1
       AND edge_kind IN ('EXTENDS', 'IMPLEMENTS', 'INHERITS')
       AND graph_version = ?2
       AND edge_id IS NULL
       AND reference_name IS NOT NULL";

/// Every symbol sharing `target_qualname`, for `collapse_exact_candidates`
/// to judge (issue #77's ambiguity rule) — deliberately no `LIMIT`, since
/// that judgment needs to see every candidate, not just the first two.
const EXACT_SQL: &str = "SELECT s.id, s.file_id, s.kind, f.path, s.signature FROM symbols s JOIN files f ON s.file_id = f.id WHERE s.qualname = ? AND s.graph_version = ? ORDER BY s.id ASC";

/// Suffix round of `resolve_import`: params are (trailing name,
/// `.{candidate}`, graph_version). `substr(.., -n)` is an exact tail comparison,
/// so `_`/`%` in names are not LIKE wildcards. `LIMIT 2` feeds
/// `Resolver::unique`'s ambiguity guard.
const IMPORT_SUFFIX_SQL: &str = "SELECT id FROM symbols
     WHERE name = ?1 AND substr(qualname, -length(?2)) = ?2 AND graph_version = ?3
     LIMIT 2";

/// Any graph version, so an incremental reindex that has not yet carried
/// the package forward still counts it. See `is_repo_python_import`.
const REPO_PYTHON_MODULE_SQL: &str = "SELECT 1 FROM symbols s JOIN files f ON s.file_id = f.id
     WHERE s.name = ? AND s.kind = 'module' AND f.language = 'python'
     LIMIT 1";

/// One round of `resolve_import_file`: an exact qualname match restricted
/// to `kind = 'module'`, so an `IMPORTS_FILE` candidate never binds to a
/// same-named non-module symbol. `LIMIT 2` feeds `Resolver::unique`'s
/// ambiguity guard.
const MODULE_EXACT_SQL: &str =
    "SELECT id FROM symbols WHERE qualname = ? AND kind = 'module' AND graph_version = ? LIMIT 2";

/// Which prepared candidate query `Resolver::unique` runs. `Exact` has its
/// own dedicated method (`Resolver::exact`) instead, since its ambiguity
/// rule needs more than "at most one row" (see `collapse_exact_candidates`);
/// `SameLang`/`AnyLang` likewise have their own dedicated methods
/// (`same_lang_lookup`/`any_lang_lookup`), for their per-row visibility /
/// case-sensitivity filtering.
#[derive(Clone, Copy)]
enum Lookup {
    ImportSuffix,
    Module,
}

/// The guarded name-fallback tier's two guards (see the module doc and
/// issue #75), bundled so `unique_by_pattern`/`same_lang_lookup` take one
/// argument instead of two loose bools.
#[derive(Clone, Copy)]
struct FallbackGuard {
    /// Refuse a `method`-kind candidate — set for a bare (receiver-less)
    /// `CALLS` edge; see `EdgeInput::bare_call`.
    exclude_method: bool,
    /// Refuse a cross-file candidate the language's `VisibilityRule`
    /// deems not visible.
    enforce_visibility: bool,
}

impl FallbackGuard {
    /// Neither guard applies — tier 4 (receiver-type/inherited
    /// resolution) and `resolve_type_symbol`'s type-name lookups.
    const NONE: FallbackGuard = FallbackGuard {
        exclude_method: false,
        enforce_visibility: false,
    };
}

/// The reference's own file and its calling symbol's qualname — the
/// `VisibilityRule` context `same_lang_lookup` needs, bundled into one
/// argument (rather than two loose ones) to keep
/// `resolve_by_name`/`unique_by_pattern`'s parameter count under
/// clippy's `too_many_arguments` threshold.
#[derive(Clone, Copy)]
struct CallerContext<'a> {
    file_path: &'a str,
    qualname: Option<&'a str>,
    symbol_id: Option<i64>,
}

/// `Resolver`'s symbol index for one language (`DeclarationIndex`).
struct LanguageIndex<'r, 'c> {
    resolver: &'r Resolver<'c>,
    lang: &'r str,
}

/// Restricts a `symbols s JOIN files f` query to symbols live at graph
/// version `?n`.
fn live_symbols(n: usize) -> String {
    format!("s.graph_version = ?{n} AND (f.deleted_version IS NULL OR f.deleted_version > ?{n})")
}

impl DeclarationIndex for LanguageIndex<'_, '_> {
    fn declarations(&self, query: DeclarationQuery<'_>) -> Result<Vec<Declaration>> {
        // (filter over ?1, ?4 = method name, ?5 = qualname suffix; ?1 the arg)
        let (filter, arg, name, suffix) = match query {
            DeclarationQuery::Callable(qualname) => (
                "s.kind IN ('function', 'method') AND s.qualname = ?1",
                qualname.to_string(),
                String::new(),
                String::new(),
            ),
            DeclarationQuery::Method(qualified) => (
                "s.kind = 'method' AND (s.qualname = ?1 OR substr(s.qualname, -length(?1) - 2) = '::' || ?1)",
                qualified.to_string(),
                String::new(),
                String::new(),
            ),
            DeclarationQuery::Type(name) => (
                "s.kind IN ('struct', 'enum', 'class', 'interface', 'record') AND s.name = ?1",
                name.to_string(),
                String::new(),
                String::new(),
            ),
            DeclarationQuery::Trait(name) => (
                "s.kind = 'trait' AND s.name = ?1",
                name.to_string(),
                String::new(),
                String::new(),
            ),
            DeclarationQuery::Member { ty, method } => (
                "s.kind = 'method' AND s.name = ?4
                 AND (s.qualname = ?1 OR substr(s.qualname, -length(?5)) = ?5)",
                format!("{ty}.{method}"),
                method.to_string(),
                format!(".{ty}.{method}"),
            ),
        };
        let mut stmt = self.resolver.conn.prepare_cached(&format!(
            "SELECT s.qualname, s.signature, s.visibility, s.file_id
             FROM symbols s JOIN files f ON s.file_id = f.id
             WHERE {filter} AND f.language = ?3 AND {}
               AND (?4 IS NOT NULL AND ?5 IS NOT NULL)",
            live_symbols(2)
        ))?;
        let rows = stmt.query_map(
            params![arg, self.resolver.graph_version, self.lang, name, suffix],
            |row| {
                Ok(Declaration {
                    qualname: row.get(0)?,
                    signature: row.get(1)?,
                    visibility: row.get(2)?,
                    file_id: row.get(3)?,
                })
            },
        )?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn is_repo_type(&self, name: &str) -> Result<bool> {
        Ok(!self.declarations(DeclarationQuery::Type(name))?.is_empty())
    }

    fn inherited_members(&self, ty: &str, method: &str) -> Result<Vec<Declaration>> {
        let conn = self.resolver.conn;
        let gv = self.resolver.graph_version;
        let own = self.declarations(DeclarationQuery::Member { ty, method })?;
        if !own.is_empty() {
            return Ok(own);
        }
        let type_ids = |name: &str| -> Result<Vec<i64>> {
            let mut stmt = conn.prepare_cached(&format!(
                "SELECT s.id FROM symbols s JOIN files f ON s.file_id = f.id
                 WHERE s.name = ?1 AND s.kind IN ('class', 'struct', 'interface', 'record')
                   AND f.language = ?3 AND {}",
                live_symbols(2)
            ))?;
            let rows = stmt.query_map(params![name, gv, self.lang], |row| row.get(0))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
        };
        let mut frontier = type_ids(qualname_trailing_name(ty))?;
        let mut seen: std::collections::HashSet<i64> = frontier.iter().copied().collect();
        for _ in 0..MAX_INHERITANCE_DEPTH {
            let mut level: Vec<(Option<i64>, String)> = Vec::new();
            let mut hierarchy = conn.prepare_cached(HIERARCHY_SQL)?;
            for &id in &frontier {
                let rows = hierarchy.query_map(params![id, gv], |row| {
                    Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, String>(1)?))
                })?;
                for row in rows {
                    level.push(row?);
                }
            }
            if level.is_empty() {
                break;
            }
            let mut found = Vec::new();
            for (id, text) in &level {
                let bound: Option<String> = match id {
                    Some(id) => conn
                        .prepare_cached("SELECT qualname FROM symbols WHERE id = ?")?
                        .query_row(params![id], |row| row.get(0))
                        .optional()?,
                    None => None,
                };
                found.extend(match bound {
                    // Already bound: its full qualname names the type.
                    Some(qualname) => self
                        .declarations(DeclarationQuery::Member {
                            ty: &qualname,
                            method,
                        })?
                        .into_iter()
                        .filter(|d| d.qualname == format!("{qualname}.{method}"))
                        .collect(),
                    None => self.declarations(DeclarationQuery::Member {
                        ty: qualname_trailing_name(text),
                        method,
                    })?,
                });
            }
            if !found.is_empty() {
                return Ok(found);
            }
            frontier = Vec::new();
            for (id, text) in level {
                let ids = match id {
                    Some(id) => vec![id],
                    None => type_ids(qualname_trailing_name(&text))?,
                };
                frontier.extend(ids.into_iter().filter(|id| seen.insert(*id)));
            }
            if frontier.is_empty() {
                break;
            }
        }
        Ok(Vec::new())
    }

    fn imports_in_scope(&self, decl: &Declaration) -> Result<ScopeImports> {
        let gv = self.resolver.graph_version;
        let mut stmt = self.resolver.conn.prepare_cached(
            "SELECT target_qualname, detail FROM edges
             WHERE kind = 'IMPORTS' AND file_id = ?1 AND graph_version = ?2
             UNION ALL
             SELECT reference_name, detail FROM unresolved_references
             WHERE edge_kind = 'IMPORTS' AND file_id = ?1 AND graph_version = ?2
             ORDER BY 1, 2",
        )?;
        let rows = stmt.query_map(params![decl.file_id, gv], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?,
                row.get::<_, Option<String>>(1)?,
            ))
        })?;
        let mut namespaces = Vec::new();
        let mut aliases = HashMap::new();
        for row in rows {
            let (target, detail) = row?;
            let alias = detail
                .and_then(|d| serde_json::from_str::<serde_json::Value>(&d).ok())
                .and_then(|d| {
                    Some((
                        d.get("alias")?.as_str()?.to_string(),
                        d.get("target")?.as_str()?.to_string(),
                    ))
                });
            match (alias, target) {
                (Some((alias, target)), _) => {
                    aliases.insert(alias, target);
                }
                (None, Some(target)) => namespaces.push(target),
                _ => {}
            }
        }
        // `App.Clients.Create` sits in `App.Clients` (or a class of that
        // name in `App`): every enclosing namespace is in scope.
        let mut scope = decl.qualname.rsplit_once('.').map(|(t, _)| t);
        while let Some((prefix, _)) = scope.and_then(|t| t.rsplit_once('.')) {
            namespaces.push(prefix.to_string());
            scope = Some(prefix);
        }
        Ok(ScopeImports {
            namespaces,
            aliases,
        })
    }
}

/// Resolves references against one graph version. Prepared statements
/// borrow `conn`, so build one per transaction.
pub(crate) struct Resolver<'c> {
    conn: &'c Connection,
    graph_version: i64,
    exact: Statement<'c>,
    same_lang: Statement<'c>,
    any_lang: Statement<'c>,
    /// `SAME_LANG_SQL`/`ANY_LANG_SQL` without `NAME_PREFILTER`, for tails
    /// that are not a plain symbol name (see `name_prefilter_applies`).
    same_lang_scan: Statement<'c>,
    any_lang_scan: Statement<'c>,
    hierarchy: Statement<'c>,
    import_suffix: Statement<'c>,
    repo_python_module: Statement<'c>,
    module_exact: Statement<'c>,
    /// Set when any tier of the current `resolve` saw 2+ candidates.
    saw_ambiguous: bool,
    /// Set only when an exact-qualname lookup found 2+ in-repo symbols it
    /// could not choose between (issue #239) -- unlike `saw_ambiguous`,
    /// never by a loose suffix/name match, so it proves the target is in
    /// this repository.
    saw_repo_ambiguous: bool,
    /// Set when `same_lang_lookup` (the guarded name-fallback tier only)
    /// found same-language, same-kind candidate(s) by name but refused
    /// every one of them as not visible — see `VisibilityRule`.
    saw_private: bool,
    /// The current reference's call arity (issue #123), set by `resolve`
    /// for a C# call; `None` for everything else. Consulted wherever
    /// several same-qualname candidates would otherwise collapse or
    /// refuse: see `arity_admits`.
    arity: Option<Arity>,
    /// Simple name of the current call's known receiver type, if any --
    /// narrows same-arity extension-method overloads.
    call_receiver: Option<String>,
    /// Lookup scope of the current call's known receiver type (C#).
    call_scope: TypeScope,
    /// Lazily resolved `files.id` of the single synthetic external
    /// pseudo-file every stub symbol belongs to (issue #80) -- see
    /// `external_file_id`. `None` until the first stub of this `Resolver`
    /// instance's lifetime is created or looked up.
    external_file_id: Option<i64>,
    /// Memo of `names_repo_entity`, keyed by (language family, name).
    repo_entity_memo: HashMap<(String, String), bool>,
    /// Rust crate roots, loaded on first use (see `rust_roots`).
    rust_roots: Option<Vec<RustRoot>>,
}

/// One Rust crate root: a file whose module is `crate`.
struct RustRoot {
    /// The name other crates import it by, for a library root (the module
    /// symbol's `crate <name>` signature).
    name: Option<String>,
    /// The directory the module tree is rooted at (repo-relative, no
    /// trailing slash; empty for a root at the repo root).
    dir: String,
}

impl<'c> Resolver<'c> {
    /// Prepare every candidate query against `conn` for `graph_version`.
    pub(crate) fn new(conn: &'c Connection, graph_version: i64) -> Result<Self> {
        Ok(Self {
            conn,
            graph_version,
            exact: conn.prepare(EXACT_SQL)?,
            same_lang: conn.prepare(SAME_LANG_SQL)?,
            any_lang: conn.prepare(ANY_LANG_SQL)?,
            same_lang_scan: conn.prepare(&SAME_LANG_SQL.replace(NAME_PREFILTER, ""))?,
            any_lang_scan: conn.prepare(&ANY_LANG_SQL.replace(NAME_PREFILTER, ""))?,
            hierarchy: conn.prepare(HIERARCHY_SQL)?,
            import_suffix: conn.prepare(IMPORT_SUFFIX_SQL)?,
            repo_python_module: conn.prepare(REPO_PYTHON_MODULE_SQL)?,
            module_exact: conn.prepare(MODULE_EXACT_SQL)?,
            saw_ambiguous: false,
            saw_repo_ambiguous: false,
            saw_private: false,
            arity: None,
            call_receiver: None,
            call_scope: TypeScope::default(),
            external_file_id: None,
            repo_entity_memo: HashMap::new(),
            rust_roots: None,
        })
    }

    /// Resolve one reference through every tier (see the module doc).
    /// `symbol_map` holds symbols inserted in the current batch that the
    /// exact and import tiers consult before SQL.
    pub(crate) fn resolve(
        &mut self,
        r: &Reference<'_>,
        symbol_map: &HashMap<String, i64>,
    ) -> Result<Resolution> {
        // A deferred argument (`new(..)` passed to a call) becomes the type of
        // the callee's parameter, then resolves as `new T(..)` would.
        if let Some(marker @ DeferredMarker::Argument(_)) = r.deferred {
            let ty = match profile_for(r.source_lang).deferred_receiver {
                Some(hook) => hook(
                    marker,
                    &LanguageIndex {
                        resolver: self,
                        lang: r.source_lang,
                    },
                )?,
                None => None,
            };
            return Ok(match ty.flatten().filter(|ty| !ty.is_empty()) {
                Some(ty) => self.resolve(
                    &Reference {
                        target_qualname: Some(&ty),
                        receiver_type: None,
                        receiver_scope: None,
                        deferred: None,
                        ..*r
                    },
                    symbol_map,
                )?,
                None => Resolution::Unresolved(UnresolvedReason::NoCandidates),
            });
        }
        // A deferred receiver (`ReceiverType::Deferred`) becomes the callee's
        // declared return type, or `""` (unresolved) -- never a guess.
        let patched_hook;
        // A language's own marker; `Some(None)` from its hook leaves the
        // receiver untracked rather than `""`, so no name-tier edge is lost.
        let hooked: Option<String>;
        let name_only = matches!(r.deferred, Some(DeferredMarker::Return(call)) if call.name_only);
        let r = match r
            .deferred
            .filter(|marker| !matches!(marker, DeferredMarker::Argument(_)))
            .and_then(|marker| {
                let hook = profile_for(r.source_lang).deferred_receiver?;
                Some(hook(
                    marker,
                    &LanguageIndex {
                        resolver: self,
                        lang: r.source_lang,
                    },
                ))
            }) {
            Some(resolved) => match resolved? {
                Some(ty) => {
                    hooked = ty;
                    patched_hook = Reference {
                        receiver_type: hooked.as_deref(),
                        receiver_scope: None,
                        deferred: None,
                        ..*r
                    };
                    &patched_hook
                }
                None => r,
            },
            None => r,
        };
        self.arity = match r.call_shape {
            Some(shape) if !shape.is_new => Some(Arity {
                args: shape.arg_count as usize,
                value_receiver: r.receiver_type.is_some(),
            }),
            _ => None,
        };
        self.call_receiver = r
            .receiver_type
            .filter(|ty| !ty.is_empty())
            .map(|ty| simple_type_name(ty).to_string());
        self.call_scope = r.receiver_scope.cloned().unwrap_or_default();
        let resolution = if name_only {
            self.resolve_name_only(r)?
        } else {
            self.resolve_tiers(r, symbol_map)?
        };
        // Issue #124: `new T(...)` names the class, but the call runs one of
        // its constructors -- bind that when exactly one matches by arity.
        if let (Some(shape), Resolution::Resolved { target_id, kind }) = (r.call_shape, resolution)
            && shape.is_new
            && let Some(ctor) = self.constructor_for(target_id, shape, r.source_file_path)?
        {
            return Ok(Resolution::Resolved {
                target_id: ctor,
                kind,
            });
        }
        // `new I()` / `: base()` never construct an interface: a name that
        // only reaches one refuses instead of binding it.
        if let (Some(shape), Resolution::Resolved { target_id, .. }) = (r.call_shape, resolution)
            && shape.is_new
        {
            let kind: Option<String> = self
                .conn
                .query_row("SELECT kind FROM symbols WHERE id = ?", [target_id], |r| {
                    r.get(0)
                })
                .optional()?;
            if kind.as_deref() == Some("interface") {
                return Ok(Resolution::Unresolved(UnresolvedReason::NoCandidates));
            }
        }
        // A Rust function reference (`take(counter)`) binds only a callable:
        // a `static`/`const`/field of that name is a value, not a call.
        if let Resolution::Resolved { target_id, .. } = resolution
            && r.edge_kind == "CALLS"
            && r.source_lang == "rust"
        {
            let kind: Option<String> = self
                .conn
                .query_row("SELECT kind FROM symbols WHERE id = ?", [target_id], |r| {
                    r.get(0)
                })
                .optional()?;
            if matches!(
                kind.as_deref(),
                Some("const" | "static" | "field" | "variable" | "property")
            ) {
                return Ok(Resolution::Unresolved(UnresolvedReason::NotCallable));
            }
        }
        Ok(resolution)
    }

    /// Resolve a call whose target text is only the called method's name
    /// (`a.B().C()`, see `DeferredReturn::name_only`) through its receiver
    /// type alone: the name says nothing about which symbol it is.
    fn resolve_name_only(&mut self, r: &Reference<'_>) -> Result<Resolution> {
        self.saw_ambiguous = false;
        self.saw_private = false;
        let ty = r.receiver_type.unwrap_or("");
        let found = match r.target_qualname {
            Some(name) if !ty.is_empty() => self.resolve_by_name(
                name,
                Some(ty),
                r.edge_kind,
                r.source_lang,
                CallerContext {
                    file_path: r.source_file_path,
                    qualname: r.source_qualname,
                    symbol_id: r.source_symbol_id,
                },
                r.bare_call,
            )?,
            _ => None,
        };
        Ok(match found {
            Some((id, kind)) => resolved(id, kind),
            None if ty.is_empty() => Resolution::Unresolved(UnresolvedReason::External),
            None if self.saw_ambiguous => Resolution::Unresolved(UnresolvedReason::Ambiguous),
            None if self.saw_private => Resolution::Unresolved(UnresolvedReason::Private),
            None => Resolution::Unresolved(UnresolvedReason::NoCandidates),
        })
    }

    /// The single constructor of class-like symbol `class_id` (its
    /// `<qualname>..ctor` symbols) that admits `shape`'s argument count.
    /// `None` -- keep the class -- when `class_id` isn't a type, declares no
    /// constructor, or 0 / 2+ of them admit the call.
    fn constructor_for(
        &mut self,
        class_id: i64,
        shape: CallShape,
        caller_file: &str,
    ) -> Result<Option<i64>> {
        let (qualname, kind, signature): (String, String, Option<String>) = self
            .conn
            .prepare_cached("SELECT qualname, kind, signature FROM symbols WHERE id = ?")?
            .query_row(params![class_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?;
        // A type's own signature is its primary-constructor parameter list
        // (`record R(int A)`), which `..ctor` symbols don't cover.
        // (Interfaces are `partial`-capable but have no constructors, so
        // unlike `PARTIAL_TYPE_KINDS` they are excluded here.)
        if !matches!(kind.as_str(), "class" | "struct" | "record")
            || has_parameter_list(signature.as_deref())
        {
            return Ok(None);
        }
        let gv = self.graph_version;
        let ctor_qualname = format!("{qualname}..ctor");
        let arity = Some(Arity {
            args: shape.arg_count as usize,
            value_receiver: false,
        });
        let candidates = query_exact_candidates(&mut self.exact, &ctor_qualname, gv, caller_file)?;
        let admitted = candidates
            .iter()
            .filter(|c| c.kind == "method" && arity_admits(arity, &c.kind, c.signature.as_deref()));
        Ok(exactly_one(admitted).map(|c| c.id))
    }

    fn resolve_tiers(
        &mut self,
        r: &Reference<'_>,
        symbol_map: &HashMap<String, i64>,
    ) -> Result<Resolution> {
        self.saw_ambiguous = false;
        self.saw_repo_ambiguous = false;
        self.saw_private = false;

        // `IMPORTS_FILE` with a populated candidate list (Python) always
        // resolves through its own ordered, `module`-kind-restricted tier
        // instead — see `resolve_import_file`'s doc. An `IMPORTS_FILE` edge
        // whose extractor never populates candidates (JS/TS, Bicep) falls
        // through unchanged, to the same exact tier every other edge kind
        // uses below.
        if r.edge_kind == "IMPORTS_FILE" && !r.import_candidates.is_empty() {
            return Ok(match self.resolve_import_file(r.import_candidates)? {
                Some(id) => resolved(id, ResolutionKind::Import),
                None if self.saw_ambiguous => Resolution::Unresolved(UnresolvedReason::Ambiguous),
                None => Resolution::Unresolved(UnresolvedReason::NoCandidates),
            });
        }

        let types_only = matches!(r.edge_kind, "IMPLEMENTS" | "EXTENDS" | "INHERITS");
        // A base-list type name carries its scope-ordered guesses (C# enclosing
        // scopes, the global namespace, then usings): the first that names a
        // type wins, like the language's own lookup -- never an ambiguity.
        if types_only && !r.import_candidates.is_empty() {
            for candidate in r.import_candidates {
                if let Some(id) = self.exact(candidate, symbol_map, r.source_file_path, true)? {
                    return Ok(resolved(id, ResolutionKind::Import));
                }
            }
        } else if !is_locally_bound_bare_call(r) {
            if let Some(qn) = r.target_qualname
                && let Some(id) = self.exact(qn, symbol_map, r.source_file_path, types_only)?
            {
                return Ok(resolved(id, ResolutionKind::Exact));
            }
            // Issue #206: a C# `using` names one namespace. Two unrelated
            // symbols sharing its qualname (e.g. a namespace and a class) are
            // a genuine collision; a later tier must not pick one of them.
            if r.edge_kind == "IMPORTS" && r.source_lang == "csharp" && self.saw_ambiguous {
                return Ok(Resolution::Unresolved(UnresolvedReason::Ambiguous));
            }
            if !types_only
                && let Some(id) = self.resolve_import(
                    r.import_candidates,
                    symbol_map,
                    r.source_lang,
                    r.source_file_path,
                )?
            {
                return Ok(resolved(id, ResolutionKind::Import));
            }
            // A Rust path through a module's `pub use .. as alias`
            // re-export, or one the global lookup found ambiguous across
            // crates: walk it inside its own crate.
            if r.source_lang == "rust"
                && matches!(r.edge_kind, "CALLS" | "USES" | "IMPORTS")
                && let Some(qn) = r.target_qualname
                && let Some(id) = self.rust_follow_path(qn, r.source_file_path)?
            {
                return Ok(resolved(id, ResolutionKind::Reexport));
            }
        }
        // `new T()` / `: base()` whose `using`s name two repo types `T` is
        // ambiguous, not external: never fall on to the external-stub tier.
        if self.saw_ambiguous && r.call_shape.is_some_and(|shape| shape.is_new) {
            return Ok(Resolution::Unresolved(UnresolvedReason::Ambiguous));
        }

        // `import_candidates` is populated only when the extractor already
        // established (from this file's own using/import directives) that
        // the receiver is bound by an import, and the import tier just
        // found no single local symbol for it. Whether that refuses the
        // name-based tiers (binding on name-uniqueness alone would risk
        // e.g. `datetime.now()` landing on an unrelated local
        // `FakeClock.now`) or falls through to them is this language's
        // `LanguageProfile::import_miss` policy.
        let refuse_names = !types_only && !r.import_candidates.is_empty() && {
            match profile_for(r.source_lang).import_miss {
                ImportMissPolicy::Refuse => true,
                ImportMissPolicy::FallThrough => false,
                ImportMissPolicy::PythonRepoHeuristic => {
                    !self.is_repo_python_import(r.import_candidates)?
                }
            }
        };
        let receiver_type = if refuse_names {
            Some("")
        } else {
            r.receiver_type
        };

        let caller = CallerContext {
            file_path: r.source_file_path,
            qualname: r.source_qualname,
            symbol_id: r.source_symbol_id,
        };
        // An unqualified call with an implicit receiver (`CallShape::implicit_this`,
        // set by an extractor whose language gives such a call an enclosing
        // type) binds to that type's own member, then its base chain, before
        // any name-wide lookup lets a same-named symbol elsewhere make it
        // ambiguous. The extractor's `Container<sep>name` target text
        // carries the enclosing type.
        let implicit_receiver = match (r.call_shape, r.target_qualname, receiver_type) {
            (Some(shape), Some(qn), None) if shape.implicit_this && r.edge_kind == "CALLS" => {
                last_qualname_separator(qn)
                    .and_then(|start| qn[..start].strip_suffix(primary_separator(r.source_lang)))
                    .map(|container| (qn, container))
            }
            _ => None,
        };
        // Innermost type first (own members, then its base chain), then each
        // lexically enclosing type (a nested type sees its outer type's
        // members), like the language's own name lookup.
        if let Some((qn, mut container)) = implicit_receiver {
            loop {
                if let Some((id, kind)) = self.resolve_by_name(
                    qn,
                    Some(container),
                    r.edge_kind,
                    r.source_lang,
                    caller,
                    r.bare_call,
                )? {
                    return Ok(resolved(id, kind));
                }
                match last_qualname_separator(container).and_then(|start| {
                    container[..start].strip_suffix(primary_separator(r.source_lang))
                }) {
                    Some(outer) => container = outer,
                    None => break,
                }
            }
        }

        let found = match r.target_qualname {
            Some(qn) => self.resolve_by_name(
                qn,
                receiver_type,
                r.edge_kind,
                r.source_lang,
                caller,
                r.bare_call,
            )?,
            None => None,
        };
        // T-SQL identifiers are case-insensitive: `EXEC DPB.Audit_Write` binds
        // to `dpb.audit_write` declared elsewhere.
        let found = match (found, r.target_qualname) {
            (None, Some(qn)) if r.source_lang == "sql" => self
                .sql_exact_ignore_case(qn)?
                .map(|id| (id, ResolutionKind::Exact)),
            (found, _) => found,
        };
        match found {
            Some((id, kind)) => Ok(resolved(id, kind)),
            // Issue #239: the exact tier found 2+ in-repo candidates (same-arity
            // overloads), so the target is in this repository, not a foreign
            // receiver: ambiguous, never a stub.
            None if refuse_names && self.saw_repo_ambiguous => {
                Ok(Resolution::Unresolved(UnresolvedReason::Ambiguous))
            }
            // Tier 3, issue #80's actual scope: the receiver is bound by an
            // import known not to resolve here -- "calls into imports known
            // to resolve outside the repo (standard library, third-party
            // packages)".
            None if refuse_names && !never_external(r) => {
                match self.stub_resolution(
                    r.source_lang,
                    r.target_qualname,
                    r.import_candidates,
                    r.bare_call,
                    false,
                )? {
                    Some(stub) => Ok(stub),
                    // The stored import candidates point into this very
                    // repository (issue #256: a renamed extension method
                    // whose callers were carried forward), so they are
                    // stale evidence, not proof of an external receiver:
                    // judge the call as a fresh parse without them would.
                    None => self.resolve_tiers(
                        &Reference {
                            import_candidates: &[],
                            ..*r
                        },
                        symbol_map,
                    ),
                }
            }
            // A builtin/unresolved receiver type with no import involved at
            // all (a local variable, e.g. `cells = []` then
            // `cells.append(1)`) is not an import known to resolve
            // elsewhere -- stub it and every other such call site would
            // collapse onto one stub named after whichever local variable's
            // call happened to create it, defeating "who calls X?". Stay
            // unresolved instead, as before #80 -- see
            // `UnresolvedReason::External`'s doc.
            None if receiver_type == Some("")
                && profile_for(r.source_lang).untyped_receiver_is_external =>
            {
                Ok(Resolution::Unresolved(UnresolvedReason::External))
            }
            None if self.saw_ambiguous => Ok(Resolution::Unresolved(UnresolvedReason::Ambiguous)),
            None if self.saw_private => Ok(Resolution::Unresolved(UnresolvedReason::Private)),
            // Tier 6 (issue #80): every tier above found nothing at all --
            // for a language whose qualified-call syntax doesn't populate
            // `import_candidates` (so `refuse_names` above never had a
            // chance to fire), a last per-language check for whether this
            // reference is unambiguously foreign. See the module doc and
            // `is_known_external_fallback`.
            None => {
                let is_external = match r.target_qualname {
                    Some(_) if never_external(r) => false,
                    Some(qn) => self.is_known_external_fallback(r.source_lang, qn)?,
                    None => false,
                };
                let stub = if is_external {
                    self.stub_resolution(
                        r.source_lang,
                        r.target_qualname,
                        r.import_candidates,
                        r.bare_call,
                        true,
                    )?
                } else {
                    None
                };
                Ok(stub.unwrap_or(Resolution::Unresolved(UnresolvedReason::NoCandidates)))
            }
        }
    }

    /// The single SQL symbol whose qualname equals `qualname` ignoring ASCII
    /// case; `None` when there is none or more than one.
    fn sql_exact_ignore_case(&mut self, qualname: &str) -> Result<Option<i64>> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT s.id FROM symbols s JOIN files f ON s.file_id = f.id
             WHERE s.qualname = ?1 COLLATE NOCASE AND s.graph_version = ?2
               AND f.language = 'sql' AND s.kind != 'module'
               AND (f.deleted_version IS NULL OR f.deleted_version > ?2) LIMIT 2",
        )?;
        let ids = stmt
            .query_map(params![qualname, self.graph_version], |r| {
                r.get::<_, i64>(0)
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(exactly_one(ids.iter()).copied())
    }

    /// The exact-qualname tier. `collapse_exact_candidates` decides when
    /// more than one symbol shares `qualname`: an overload set (same file,
    /// same kind) collapses to the lowest id, anything else refuses rather
    /// than guess — issue #77's ambiguity rule, so incremental and fresh
    /// always agree on an ambiguous name.
    ///
    /// `types_only` is set for IMPLEMENTS/EXTENDS/INHERITS: the target text
    /// names a type, so a same-named file `module`/`namespace` (C#
    /// `IPublisher.cs` -> module `IPublisher`) must not win (issue #122).
    /// The in-batch map carries no kind, so it is skipped for those edges.
    fn exact(
        &mut self,
        qualname: &str,
        symbol_map: &HashMap<String, i64>,
        caller_file: &str,
        types_only: bool,
    ) -> Result<Option<i64>> {
        if !types_only && let Some(&id) = symbol_map.get(qualname) {
            return Ok(Some(id));
        }
        let gv = self.graph_version;
        let mut candidates = query_exact_candidates(&mut self.exact, qualname, gv, caller_file)?;
        if types_only {
            candidates.retain(|c| !matches!(c.kind.as_str(), "module" | "namespace"));
        }
        // Issue #123: an overload set is told apart by the call's arity, and
        // never collapsed to "the lowest id" -- 2+ admitted overloads refuse.
        if self.arity.is_some() && candidates.len() > 1 {
            let mut admitted: Vec<&ExactCandidate> = candidates
                .iter()
                .filter(|c| arity_admits(self.arity, &c.kind, c.signature.as_deref()))
                .collect();
            // Same-arity extension overloads (`ToDb(this A)` / `ToDb(this
            // B)`) are told apart by the call receiver's known type.
            if admitted.len() > 1
                && let Some(receiver) = self.call_receiver.as_deref()
                && let Some(only) = exactly_one(admitted.iter().filter(|c| {
                    c.signature
                        .as_deref()
                        .and_then(extension_receiver_type)
                        .is_some_and(|ty| ty == receiver)
                }))
            {
                admitted = vec![*only];
            }
            if admitted.len() > 1 {
                if let Some(id) = canonical_multi_file(admitted.iter().copied()) {
                    return Ok(Some(id));
                }
                self.saw_ambiguous = true;
                self.saw_repo_ambiguous = true;
            }
            return Ok(exactly_one(admitted.into_iter()).map(|c| c.id));
        }
        let resolved = collapse_exact_candidates(&candidates);
        if resolved.is_none() && candidates.len() > 1 {
            self.saw_ambiguous = true;
            self.saw_repo_ambiguous = true;
        }
        Ok(resolved)
    }

    /// Every Rust crate root (a file whose module is `crate`), loaded once.
    fn rust_roots(&mut self) -> Result<&[RustRoot]> {
        if self.rust_roots.is_none() {
            let mut stmt = self.conn.prepare(
                "SELECT f.path, s.signature FROM symbols s JOIN files f ON f.id = s.file_id
                 WHERE s.graph_version = ?1 AND s.kind = 'module' AND s.qualname = 'crate'
                   AND f.language = 'rust'",
            )?;
            let roots = stmt
                .query_map(params![self.graph_version], |row| {
                    let path: String = row.get(0)?;
                    let signature: Option<String> = row.get(1)?;
                    Ok(RustRoot {
                        name: signature
                            .as_deref()
                            .and_then(|sig| sig.strip_prefix("crate "))
                            .map(str::to_string),
                        dir: path.rsplit_once('/').map_or("", |(dir, _)| dir).to_string(),
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            self.rust_roots = Some(roots);
        }
        Ok(self.rust_roots.as_deref().unwrap_or_default())
    }

    /// The root directory of the crate `file_path` belongs to: the deepest
    /// crate-root directory containing it.
    fn rust_crate_dir(&mut self, file_path: &str) -> Result<Option<String>> {
        Ok(self
            .rust_roots()?
            .iter()
            .filter(|root| {
                root.dir.is_empty()
                    || file_path
                        .strip_prefix(root.dir.as_str())
                        .is_some_and(|rest| rest.starts_with('/'))
            })
            .max_by_key(|root| root.dir.len())
            .map(|root| root.dir.clone()))
    }

    /// Resolve a Rust path rooted at `crate::` by walking the caller's own
    /// crate: an exact qualname among that crate's files, else the
    /// re-export (`pub use target as name`) a module on the path binds.
    /// `None` when the path leaves the crate, or anything is ambiguous.
    fn rust_follow_path(&mut self, path: &str, caller_file: &str) -> Result<Option<i64>> {
        let segments: Vec<&str> = path.split("::").collect();
        if segments.len() < 2 {
            return Ok(None);
        }
        let dir = if segments[0] == "crate" {
            self.rust_crate_dir(caller_file)?
        } else {
            self.rust_crate_dir_by_name(segments[0])?
        };
        match dir {
            Some(dir) => self.rust_walk(&dir, &segments[1..], 0),
            None => Ok(None),
        }
    }

    /// The root directory of the one library crate named `name` (as other
    /// crates write it); `None` when no or several repo crates have it.
    fn rust_crate_dir_by_name(&mut self, name: &str) -> Result<Option<String>> {
        let mut dirs = self
            .rust_roots()?
            .iter()
            .filter(|root| root.name.as_deref() == Some(name))
            .map(|root| root.dir.as_str());
        Ok(match (dirs.next(), dirs.next()) {
            (Some(dir), None) => Some(dir.to_string()),
            _ => None,
        })
    }

    /// `rem` is the path below the root of the crate rooted at `dir`.
    fn rust_walk(&mut self, dir: &str, rem: &[&str], depth: usize) -> Result<Option<i64>> {
        if rem.is_empty() || depth > 8 {
            return Ok(None);
        }
        let qualname = format!("crate::{}", rem.join("::"));
        let gv = self.graph_version;
        let hits = query_exact_candidates(&mut self.exact, &qualname, gv, "")?;
        let mut in_crate = Vec::new();
        for hit in hits {
            if self.rust_crate_dir(&hit.path)?.as_deref() == Some(dir) {
                in_crate.push(hit);
            }
        }
        if let Some(id) = collapse_exact_candidates(&in_crate) {
            return Ok(Some(id));
        }
        if in_crate.len() > 1 {
            return Ok(None);
        }
        for k in 1..=rem.len() {
            let module = if k == 1 {
                "crate".to_string()
            } else {
                format!("crate::{}", rem[..k - 1].join("::"))
            };
            let Some(target) = self.rust_reexport_target(dir, &module, rem[k - 1])? else {
                continue;
            };
            let mut next: Vec<&str> = target.split("::").collect();
            next.extend_from_slice(&rem[k..]);
            let next_dir = if next[0] == "crate" {
                Some(dir.to_string())
            } else {
                self.rust_crate_dir_by_name(next[0])?
            };
            return match next_dir {
                Some(next_dir) => self.rust_walk(&next_dir, &next[1..], depth + 1),
                None => Ok(None),
            };
        }
        Ok(None)
    }

    /// The single path module `module` (in the crate rooted at `dir`)
    /// binds `name` to through a `use` declaration, `None` when it binds
    /// none or two different ones.
    fn rust_reexport_target(
        &mut self,
        dir: &str,
        module: &str,
        name: &str,
    ) -> Result<Option<String>> {
        // An unresolved `use` (its target is itself a re-export, or another
        // crate's item) lives only in the unresolved-reference store.
        let mut stmt = self.conn.prepare_cached(
            "SELECT e.target_qualname, f.path
             FROM edges e
             JOIN symbols s ON s.id = e.source_symbol_id
             JOIN files f ON f.id = e.file_id
             WHERE e.graph_version = ?1 AND e.kind = 'IMPORTS' AND s.qualname = ?2
               AND e.target_qualname IS NOT NULL
               AND (e.detail = ?3
                    OR (e.detail IS NULL AND (e.target_qualname = ?4
                        OR substr(e.target_qualname, -length(?5)) = ?5)))
             UNION
             SELECT ur.reference_name, f.path
             FROM unresolved_references ur
             JOIN symbols s ON s.id = ur.source_symbol_id
             JOIN files f ON f.id = ur.file_id
             WHERE ur.graph_version = ?1 AND ur.edge_kind = 'IMPORTS' AND s.qualname = ?2
               AND ur.reference_name IS NOT NULL
               AND (ur.detail = ?3
                    OR (ur.detail IS NULL AND (ur.reference_name = ?4
                        OR substr(ur.reference_name, -length(?5)) = ?5)))",
        )?;
        let rows = stmt
            .query_map(
                params![
                    self.graph_version,
                    module,
                    format!("as {name}"),
                    name,
                    format!("::{name}")
                ],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut targets = Vec::new();
        for (target, path) in rows {
            if self.rust_crate_dir(&path)?.as_deref() == Some(dir) && !targets.contains(&target) {
                targets.push(target);
            }
        }
        Ok(match targets.as_slice() {
            [only] => Some(only.clone()),
            _ => None,
        })
    }

    /// Ambiguity guard for every name-based lookup: bind only when exactly
    /// one candidate survives the query's filters. A second row means the
    /// name is ambiguous (e.g. `.append` matching both `list.append` and a
    /// domain `EventStore.append`), so return `None` rather than whichever
    /// row SQLite returned first.
    ///
    /// ponytail: candidate-count(<=1) is the cheapest signal that fixes
    /// over-binding without receiver-type inference; if it proves too
    /// coarse, resolving the receiver's type first is the upgrade path, not
    /// a bigger threshold.
    fn unique(&mut self, lookup: Lookup, query_params: &[&dyn ToSql]) -> Result<Option<i64>> {
        let stmt = match lookup {
            Lookup::ImportSuffix => &mut self.import_suffix,
            Lookup::Module => &mut self.module_exact,
        };
        let mut rows = stmt.query(query_params)?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let id: i64 = row.get(0)?;
        if rows.next()?.is_some() {
            self.saw_ambiguous = true;
            return Ok(None);
        }
        Ok(Some(id))
    }

    /// Match `name` in the source language's own same-language round
    /// (patterns built from `profile_for(source_lang)`'s separators), then
    /// — Bridge Edge kinds only — in any language, using
    /// `any_lang_patterns` (always both `.` and `::`: the target's
    /// language isn't known in advance for a bridge match). The any-
    /// language round never applies `guard`: it's specific to the guarded,
    /// same-language-only fallback (see the module doc and issue #75); a
    /// Bridge Edge crossing languages already has its own, separate
    /// cross-language contract.
    fn unique_by_pattern(
        &mut self,
        name: &str,
        any_lang_patterns: (&str, &str),
        source_lang: &str,
        edge_kind: &str,
        guard: FallbackGuard,
        caller: CallerContext<'_>,
    ) -> Result<Option<i64>> {
        let profile = profile_for(source_lang);
        let (same_p1, same_p2) = same_lang_patterns(name, &profile);
        let same = self.same_lang_lookup(name, (&same_p1, &same_p2), guard, source_lang, caller)?;
        if same.is_some() || !is_bridge_edge_kind(edge_kind) {
            return Ok(same);
        }
        self.any_lang_lookup(name, any_lang_patterns)
    }

    /// Iterate rows checking `qualname` (at column `col_qualname`) case-
    /// sensitively against `name`. For each case-sensitive match, call
    /// `on_match` with the row; if it returns true, count that as a "real"
    /// match. Returns true if a second "real" match is found (ambiguity).
    fn check_case_sensitive_matches<F>(
        rows: &mut rusqlite::Rows,
        name: &str,
        col_qualname: usize,
        mut on_match: F,
    ) -> Result<bool>
    where
        F: FnMut(&rusqlite::Row) -> Result<bool>,
    {
        let mut real_match_count = 0usize;
        while let Some(row) = rows.next()? {
            let qualname: String = row.get(col_qualname)?;
            if !matches_name_case_sensitive(&qualname, name) {
                continue;
            }
            if on_match(row)? {
                real_match_count += 1;
                if real_match_count >= 2 {
                    return Ok(true); // ambiguous
                }
            }
        }
        Ok(false) // not ambiguous
    }

    /// The any-language round of the guarded name-fallback tier (Bridge
    /// Edge kinds only — see `unique_by_pattern`'s doc): same
    /// case-sensitive re-check as `same_lang_lookup` (issue #110), just
    /// without a `VisibilityRule` — a cross-language bridge match has no
    /// per-language visibility contract to apply. Consumes the result
    /// lazily, same early-exit-on-second-match shape as `same_lang_lookup`
    /// (see `ANY_LANG_SQL`'s doc for why it carries no `LIMIT`).
    fn any_lang_lookup(&mut self, name: &str, patterns: (&str, &str)) -> Result<Option<i64>> {
        let arity = self.arity;
        let tail = qualname_trailing_name(name);
        let gv = self.graph_version;
        let named = named_params! {
            ":tail": tail, ":name": name, ":p1": patterns.0, ":p2": patterns.1, ":gv": gv,
        };
        let mut rows = if name_prefilter_applies(tail) {
            self.any_lang.query(named)?
        } else {
            self.any_lang_scan.query(&named[1..])?
        };
        let mut matched: Option<i64> = None;
        let is_ambiguous = Self::check_case_sensitive_matches(&mut rows, name, 1, |row| {
            // Issue #186: a C# overload of the wrong arity was never a
            // candidate (only C# signatures are arity-readable).
            let language: Option<String> = row.get(4)?;
            if language.as_deref() == Some("csharp") {
                let kind: String = row.get(2)?;
                let signature: Option<String> = row.get(3)?;
                if !arity_admits(arity, &kind, signature.as_deref()) {
                    return Ok(false);
                }
            }
            matched = Some(row.get(0)?);
            Ok(true)
        })?;
        if is_ambiguous {
            self.saw_ambiguous = true;
            return Ok(None);
        }
        Ok(matched)
    }

    /// Same-language candidate lookup shared by tier 4
    /// (`resolve_type_symbol`'s type-name lookups, `FallbackGuard::NONE`)
    /// and tier 5, the guarded name fallback (`unique_by_pattern`, a real
    /// `guard`) — see the module doc. Runs `SAME_LANG_SQL` (which already
    /// excludes `method`-kind rows in the query itself when
    /// `guard.exclude_method`, not as a post-filter — see that constant's
    /// doc for why), re-checks each row's `qualname` case-sensitively
    /// against `name` (`SAME_LANG_SQL`'s `LIKE` clauses alone would let it
    /// through case-insensitively — issue #110), applies
    /// `guard.enforce_visibility`'s `VisibilityRule` per row, and requires
    /// exactly one survivor.
    ///
    /// Consumes the result lazily and stops as soon as a *second* visible
    /// candidate is found — correctness doesn't need to see the rest once
    /// ambiguity is certain, and this keeps a repo-wide decoy pile (a
    /// common short name matching hundreds of symbols) from costing more
    /// than the two rows needed to prove ambiguity. Finding 0 or 1 visible
    /// candidates does require draining every matching row, but that set
    /// is exactly the rows `SAME_LANG_SQL` already scoped to this
    /// name/kind/language — not an arbitrarily large one.
    ///
    /// A same-file candidate is always visible, regardless of
    /// `guard.enforce_visibility` — a symbol is never private to its own
    /// file. When nothing survives filtering but at least one same-name
    /// row existed, that's specifically a privacy refusal (`saw_private`),
    /// not a plain miss — `resolve` reports `Unresolved(Private)` for it,
    /// distinct from `NoCandidates`/`Ambiguous`. A row that only matched
    /// `SAME_LANG_SQL`'s `LIKE` clauses case-insensitively (never the
    /// case-sensitive re-check) doesn't count toward either outcome — it
    /// was never really a name match.
    fn same_lang_lookup(
        &mut self,
        name: &str,
        patterns: (&str, &str),
        guard: FallbackGuard,
        source_lang: &str,
        caller: CallerContext<'_>,
    ) -> Result<Option<i64>> {
        let visibility_rule = profile_for(source_lang).visibility;
        let arity = self.arity;
        let tail = qualname_trailing_name(name);
        let gv = self.graph_version;
        let named = named_params! {
            ":tail": tail, ":name": name, ":p1": patterns.0, ":p2": patterns.1,
            ":exclude_method": i64::from(guard.exclude_method), ":gv": gv, ":lang": source_lang,
        };
        let mut rows = if name_prefilter_applies(tail) {
            self.same_lang.query(named)?
        } else {
            self.same_lang_scan.query(&named[1..])?
        };
        let mut kind_eligible = 0usize;
        let mut visible: Vec<i64> = Vec::new();
        let is_ambiguous = Self::check_case_sensitive_matches(&mut rows, name, 2, |row| {
            let id: i64 = row.get(0)?;
            let visibility: Option<String> = row.get(1)?;
            let qualname: String = row.get(2)?;
            let file_path: String = row.get(3)?;
            let kind: String = row.get(4)?;
            let signature: Option<String> = row.get(5)?;
            // Issue #123: an overload of the wrong arity was never a candidate.
            if !arity_admits(arity, &kind, signature.as_deref()) {
                return Ok(false);
            }
            // `SAME_LANG_SQL` already excludes `method`-kind rows when
            // `guard.exclude_method` — every row reaching here is kind-eligible.
            if caller.symbol_id == Some(id) {
                return Ok(false);
            }
            if is_fixture_path(&file_path) && !is_fixture_path(caller.file_path) {
                return Ok(false);
            }
            kind_eligible += 1;
            let ok = !guard.enforce_visibility
                || file_path == caller.file_path
                || visibility_rule.is_visible(
                    visibility.as_deref(),
                    &qualname,
                    &file_path,
                    caller.file_path,
                    caller.qualname,
                );
            if ok {
                visible.push(id);
                Ok(true) // count as a "real" match
            } else {
                Ok(false) // case-sensitive match but not visible
            }
        })?;
        if is_ambiguous {
            self.saw_ambiguous = true;
        }

        match visible.len() {
            1 => Ok(Some(visible[0])),
            0 if kind_eligible > 0 => {
                self.saw_private = true;
                Ok(None)
            }
            // Only reachable with 0 or > 1 (when `saw_ambiguous` was set).
            _ => Ok(None),
        }
    }

    /// Tiers 4–5, gated by the reference's `receiver_type` signal (see
    /// `edges.receiver_type` / `ReceiverType`):
    ///
    /// - `None` = not tracked by the extractor: two-segment, then bare name.
    /// - `Some("")` = tracked but builtin/unresolved, or refused by the
    ///   known-external rule: no lookup at all.
    /// - `Some(ty)` = known receiver type: `ty`'s own method, else its
    ///   closest declaring ancestor. No bare-name fallback — an unmatched
    ///   known type stays unbound rather than guessing.
    ///
    /// `caller` and `bare_call` feed the guarded fallback's (tier 5, the
    /// `None` arm) visibility and bare-call guards only — tier 4 (the
    /// `Some` arm) passes `FallbackGuard::NONE` and never consults either.
    fn resolve_by_name(
        &mut self,
        target_qualname: &str,
        receiver_type: Option<&str>,
        edge_kind: &str,
        source_lang: &str,
        caller: CallerContext<'_>,
        bare_call: bool,
    ) -> Result<Option<(i64, ResolutionKind)>> {
        let source_lang = resolution_language_family(source_lang);
        match receiver_type {
            Some("") => Ok(None),

            Some(known_type) => {
                // C# interface receivers keep the qualifier and closed type
                // arguments they were declared with (`N1.IA<int>`); the
                // arguments only discriminate dispatch, never resolution.
                let scope = self.call_scope.clone();
                let known_type = known_type.split('<').next().unwrap_or(known_type);
                let method = qualname_trailing_name(target_qualname);
                // A Rust receiver type the caller's `use` pinned to one path
                // (`crate::util::RegexMatcher`): that exact type's member,
                // else its trait default / impl-trait ancestors, before the
                // bare name -- which says nothing when four types share it.
                let rust_path =
                    (source_lang == "rust" && known_type.contains("::")).then_some(known_type);
                if let Some(path) = rust_path {
                    let hits = query_exact_candidates(
                        &mut self.exact,
                        &format!("{path}::{method}"),
                        self.graph_version,
                        caller.file_path,
                    )?;
                    if let Some(id) = collapse_exact_candidates(&hits) {
                        return Ok(Some((id, ResolutionKind::ReceiverType)));
                    }
                    let bare = path.rsplit("::").next().unwrap_or(path);
                    if let Some(id) = self.resolve_via_inheritance(
                        (path, bare),
                        method,
                        source_lang,
                        edge_kind,
                        caller,
                    )? {
                        return Ok(Some((id, ResolutionKind::Inherited)));
                    }
                }
                // A bare Rust receiver type declared in the caller's own
                // file is that type: a same-named type in another crate is
                // not in scope unless imported (an import pins the path
                // above).
                if source_lang == "rust"
                    && rust_path.is_none()
                    && let Some(id) =
                        self.rust_enclosing_module_member(known_type, method, caller)?
                {
                    return Ok(Some((id, ResolutionKind::ReceiverType)));
                }
                let known_type = rust_path
                    .and_then(|p| p.rsplit("::").next())
                    .unwrap_or(known_type);
                if let Some(id) =
                    self.scoped_member(&scope, known_type, method, caller.file_path)?
                {
                    return Ok(Some((id, ResolutionKind::ReceiverType)));
                }
                // A namespace-qualified receiver (`N1.IA`) binds to exactly
                // that type's member: two same-named interfaces in other
                // namespaces are not candidates.
                if known_type.contains('.')
                    && let Some(id) = self.qualified_member(known_type, method, caller.file_path)?
                {
                    return Ok(Some((id, ResolutionKind::ReceiverType)));
                }
                let full_type = known_type;
                let known_type = known_type.rsplit('.').next().unwrap_or(known_type);
                let seed = format!("{known_type}{}{method}", primary_separator(source_lang));
                let Some((seg, dot, colons)) = two_segment_qualname_patterns(&seed) else {
                    return Ok(None);
                };
                if let Some(id) = self.unique_by_pattern(
                    &seg,
                    (&dot, &colons),
                    source_lang,
                    edge_kind,
                    FallbackGuard::NONE,
                    caller,
                )? {
                    return Ok(Some((id, ResolutionKind::ReceiverType)));
                }
                // The receiver's own type declares no matching method (or
                // the match there was itself ambiguous) — walk its ancestors.
                Ok(self
                    .resolve_via_inheritance(
                        (full_type, known_type),
                        method,
                        source_lang,
                        edge_kind,
                        caller,
                    )?
                    .map(|id| (id, ResolutionKind::Inherited)))
            }

            None => {
                // A bare call (no receiver at all, syntactically) can never
                // dispatch to a `method` — only a receiver decides which
                // instance's method runs. Gated to `CALLS` only: other edge
                // kinds (RPC_CALL, HTTP_CALL, CHANNEL_*, XREF, ...) have
                // their own detection and don't set `bare_call`, so this
                // never restricts them (see `EdgeInput::bare_call`).
                //
                // A Rust `use` path likewise never names a method or
                // associated fn, so an `IMPORTS` edge refuses them too
                // (else `use crate::util::f` binds to an unrelated `T::f`
                // until `util` is indexed).
                let guard = FallbackGuard {
                    exclude_method: (edge_kind == "CALLS" && bare_call)
                        || (edge_kind == "IMPORTS" && source_lang == "rust"),
                    enforce_visibility: true,
                };
                if let Some((seg, dot, colons)) = two_segment_qualname_patterns(target_qualname)
                    && let Some(id) = self.unique_by_pattern(
                        &seg,
                        (&dot, &colons),
                        source_lang,
                        edge_kind,
                        guard,
                        caller,
                    )?
                {
                    return Ok(Some((id, ResolutionKind::TwoSegment)));
                }
                // A `::` path that isn't `crate::`-rooted (`std::fs::x`,
                // `String::new`) names something outside this repo unless
                // its type segment is a repo type (`Quiet::announce` where
                // `Quiet` is local and inherits a default method). Its
                // trailing name alone says nothing about a same-named
                // crate symbol (issue #102).
                if source_lang == "rust"
                    && is_foreign_rust_path(target_qualname)
                    && !self.rust_path_names_repo_type(target_qualname, caller.file_path)?
                {
                    return Ok(None);
                }
                let (name, dot, colons) = fuzzy_qualname_patterns(target_qualname);
                Ok(self
                    .unique_by_pattern(
                        name,
                        (&dot, &colons),
                        source_lang,
                        edge_kind,
                        guard,
                        caller,
                    )?
                    .map(|id| (id, ResolutionKind::BareName)))
            }
        }
    }

    /// An unqualified receiver type looked up like C# does (see
    /// [`TypeScope`]): the first enclosing scope declaring `{scope}.{ty}.{member}`
    /// wins, then the global namespace, then exactly one `using` namespace
    /// (two is a compile error in C#, so stays ambiguous). `None` falls
    /// through to the name-based lookup.
    fn scoped_member(
        &mut self,
        scope: &TypeScope,
        ty: &str,
        member: &str,
        caller_file: &str,
    ) -> Result<Option<i64>> {
        if scope.enclosing.is_empty() && scope.usings.is_empty() {
            return Ok(None);
        }
        let gv = self.graph_version;
        let lookup = |this: &mut Self, ns: &str| -> Result<Option<i64>> {
            let full = if ns.is_empty() {
                format!("{ty}.{member}")
            } else {
                format!("{ns}.{ty}.{member}")
            };
            let hits = query_exact_candidates(&mut this.exact, &full, gv, caller_file)?;
            Ok(collapse_exact_candidates(&hits))
        };
        for ns in scope.enclosing.iter().map(String::as_str).chain([""]) {
            if let Some(id) = lookup(self, ns)? {
                return Ok(Some(id));
            }
        }
        let mut found = None;
        for ns in &scope.usings {
            if let Some(id) = lookup(self, ns)? {
                if found.is_some_and(|f| f != id) {
                    self.saw_ambiguous = true;
                    return Ok(None);
                }
                found = Some(id);
            }
        }
        Ok(found)
    }

    /// `ty::member` for a Rust type `ty` declared in the caller's own file:
    /// the caller's qualname minus its trailing segments (the module, or an
    /// inline `mod` it sits in), nearest first, keeping only a symbol in the
    /// caller's file. A same-named type in another crate (another
    /// integration-test file, another workspace member) is never a
    /// candidate; a type in another file is reached through the `use` path
    /// the extractor pins instead.
    fn rust_enclosing_module_member(
        &mut self,
        ty: &str,
        member: &str,
        caller: CallerContext<'_>,
    ) -> Result<Option<i64>> {
        let Some(mut scope) = caller.qualname else {
            return Ok(None);
        };
        let gv = self.graph_version;
        while let Some((parent, _)) = scope.rsplit_once("::") {
            scope = parent;
            let hits = query_exact_candidates(
                &mut self.exact,
                &format!("{scope}::{ty}::{member}"),
                gv,
                caller.file_path,
            )?;
            let own: Vec<ExactCandidate> = hits
                .into_iter()
                .filter(|c| c.path == caller.file_path)
                .collect();
            if let Some(id) = collapse_exact_candidates(&own) {
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    /// `{type_path}.{member}` written as a (possibly partially) qualified
    /// receiver type: the exact qualname, else the single symbol whose
    /// qualname ends with `.{type_path}.{member}` (the receiver's own
    /// enclosing namespace prefix is not repeated). `None` when nothing or
    /// several match.
    fn qualified_member(
        &mut self,
        type_path: &str,
        member: &str,
        caller_file: &str,
    ) -> Result<Option<i64>> {
        let full = format!("{type_path}.{member}");
        let gv = self.graph_version;
        let exact = query_exact_candidates(&mut self.exact, &full, gv, caller_file)?;
        if let Some(id) = collapse_exact_candidates(&exact) {
            return Ok(Some(id));
        }
        let saw = self.saw_ambiguous;
        let found = self.unique(
            Lookup::ImportSuffix,
            params![member, format!(".{full}"), gv],
        )?;
        self.saw_ambiguous = saw;
        Ok(found)
    }

    /// Whether a foreign-looking Rust path's second-to-last segment is a
    /// unique repo type. Never true for a `std`/`core`/`alloc` root. A type
    /// name matching 2+ repo types leaves `saw_ambiguous` set, so `resolve`
    /// reports `Unresolved(Ambiguous)` rather than stubbing it external.
    fn rust_path_names_repo_type(&mut self, path: &str, file_path: &str) -> Result<bool> {
        let mut segments = path.rsplit("::").skip(1);
        let (Some(type_name), root) = (segments.next(), path.split("::").next()) else {
            return Ok(false);
        };
        if matches!(root, Some("std" | "core" | "alloc")) {
            return Ok(false);
        }
        let private = self.saw_private;
        let found = self.resolve_type_symbol(type_name, "rust", file_path)?;
        self.saw_private = private;
        Ok(found.is_some())
    }

    /// Resolve a bare type name (a receiver's inferred type, or an
    /// ancestor's `target_qualname` text) to the single symbol declaring
    /// it. Same-language only — a class hierarchy never crosses languages.
    /// `FallbackGuard::NONE`: tier 4, neither guard applies.
    fn resolve_type_symbol(
        &mut self,
        type_name: &str,
        source_lang: &str,
        source_file_path: &str,
    ) -> Result<Option<i64>> {
        let name = qualname_trailing_name(type_name);
        let (p1, p2) = same_lang_patterns(name, &profile_for(source_lang));
        self.same_lang_lookup(
            name,
            (&p1, &p2),
            FallbackGuard::NONE,
            source_lang,
            CallerContext {
                file_path: source_file_path,
                qualname: None,
                symbol_id: None,
            },
        )
    }

    /// When a receiver's own type declares no matching method, walk up its
    /// recorded EXTENDS/IMPLEMENTS/INHERITS edges for the ancestor that
    /// does — the method a call through that receiver would dispatch to.
    ///
    /// Breadth-first, level by level, in declaration order. A closer
    /// ancestor always wins over a farther one. Two ancestors at the *same*
    /// level both declaring it (C# multiple interfaces, Python multiple
    /// bases) is a genuine ambiguity and is refused. Bounded by
    /// `MAX_INHERITANCE_DEPTH`.
    fn resolve_via_inheritance(
        &mut self,
        (full_type, known_type): (&str, &str),
        method: &str,
        source_lang: &str,
        edge_kind: &str,
        caller: CallerContext<'_>,
    ) -> Result<Option<i64>> {
        // A type declared in several files (a C# `partial` class) has one
        // symbol per part, each carrying only its own base list: every part
        // of the exactly-named type is a root. Otherwise the bare type name.
        let gv = self.graph_version;
        let rust = source_lang == "rust";
        let parts: Vec<i64> = if full_type.contains('.') || full_type.contains("::") {
            query_exact_candidates(&mut self.exact, full_type, gv, caller.file_path)?
                .into_iter()
                .filter(|c| {
                    matches!(c.kind.as_str(), "class" | "struct" | "record" | "interface")
                        || (rust && matches!(c.kind.as_str(), "enum" | "trait"))
                })
                .map(|c| c.id)
                .collect()
        } else {
            Vec::new()
        };
        // An exact Rust path names its one type; a bare name may be shared.
        let mut frontier = if parts.len() > 1 || (rust && parts.len() == 1) {
            parts
        } else {
            let Some(root_id) =
                self.resolve_type_symbol(known_type, source_lang, caller.file_path)?
            else {
                return Ok(None);
            };
            vec![root_id]
        };
        let mut seen: std::collections::HashSet<i64> = frontier.iter().copied().collect();

        for _ in 0..MAX_INHERITANCE_DEPTH {
            // This level's direct ancestors, in declaration order, across
            // every symbol reached at the previous level.
            let mut level: Vec<(Option<i64>, String)> = Vec::new();
            for &sym_id in &frontier {
                let rows = self
                    .hierarchy
                    .query_map(params![sym_id, self.graph_version], |row| {
                        Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, String>(1)?))
                    })?;
                for row in rows {
                    level.push(row?);
                }
            }
            if level.is_empty() {
                break;
            }

            let mut matches: Vec<i64> = Vec::new();
            for (_, ancestor_qualname) in &level {
                let ancestor_name = qualname_trailing_name(ancestor_qualname);
                let seed = format!("{ancestor_name}{}{method}", primary_separator(source_lang));
                let Some((seg, dot, colons)) = two_segment_qualname_patterns(&seed) else {
                    continue;
                };
                if let Some(id) = self.unique_by_pattern(
                    &seg,
                    (&dot, &colons),
                    source_lang,
                    edge_kind,
                    FallbackGuard::NONE,
                    caller,
                )? {
                    matches.push(id);
                }
            }

            match matches.len() {
                0 => {}
                1 => return Ok(Some(matches[0])),
                _ => {
                    // two unrelated ancestors both declare it: refuse
                    self.saw_ambiguous = true;
                    return Ok(None);
                }
            }

            // Nobody at this level declares it — descend. Prefer each
            // ancestor's already-resolved target_symbol_id; re-resolve by
            // name only while it's still NULL (e.g. within the same
            // insert_edges pass, before the ancestor's file is processed).
            let mut next_frontier = Vec::new();
            for (ancestor_symbol_id, ancestor_qualname) in &level {
                let next_id = match ancestor_symbol_id {
                    Some(id) => Some(*id),
                    None => self.resolve_type_symbol(
                        qualname_trailing_name(ancestor_qualname),
                        source_lang,
                        caller.file_path,
                    )?,
                };
                if let Some(id) = next_id
                    && seen.insert(id)
                {
                    next_frontier.push(id);
                }
            }
            if next_frontier.is_empty() {
                break;
            }
            frontier = next_frontier;
        }

        Ok(None)
    }

    /// Resolve a call's import-qualified candidate qualnames (see
    /// `EdgeInput::import_candidates`), binding only when precisely one
    /// distinct symbol is found across *every* candidate. Each candidate is
    /// an exact-qualname lookup, so this is authoritative when it hits.
    ///
    /// Only when *no* candidate hits exactly, and this language's profile
    /// says suffix matching is meaningful for it
    /// (`LanguageProfile::import_suffix_matching`), a second round retries
    /// each one as a path suffix (`IMPORT_SUFFIX_SQL`), same
    /// one-distinct-hit rule. Needed for Python, where a file's module
    /// qualname is its repo-relative path (`py.pkg.src.pkg.mod`) while the
    /// import names the installed package path (`pkg.mod`). Still the full
    /// import path, never a bare name.
    fn resolve_import(
        &mut self,
        candidates: &[String],
        symbol_map: &HashMap<String, i64>,
        source_lang: &str,
        caller_file: &str,
    ) -> Result<Option<i64>> {
        let rounds: &[bool] = if profile_for(source_lang).import_suffix_matching {
            &[true, false]
        } else {
            &[true]
        };
        for &exact_round in rounds {
            let mut found: Option<i64> = None;
            for candidate in candidates {
                let id = if exact_round {
                    self.exact(candidate, symbol_map, caller_file, false)?
                } else {
                    let name = qualname_trailing_name(candidate);
                    let suffix = format!(".{candidate}");
                    let gv = self.graph_version;
                    self.unique(Lookup::ImportSuffix, params![name, suffix, gv])?
                };
                let Some(id) = id else { continue };
                match found {
                    None => found = Some(id),
                    Some(existing) if existing == id => {}
                    Some(_) => {
                        self.saw_ambiguous = true;
                        // Two exact-round candidates naming different real
                        // symbols are in-repo; suffix matches are loose.
                        self.saw_repo_ambiguous |= exact_round;
                        return Ok(None);
                    }
                }
            }
            if found.is_some() {
                return Ok(found);
            }
        }
        if !profile_for(source_lang).import_member_fallback {
            return Ok(None);
        }
        let mut found: Option<i64> = None;
        for candidate in candidates {
            // Walk up one segment at a time (`api.users.list` -> `api.users`
            // -> `api`) until a symbol hits; only a const/variable binds, any
            // other kind (class, module, ...) ends the walk unbound.
            let mut rest = candidate.as_str();
            let mut hit = None;
            while let Some((parent, _)) = rest.rsplit_once('.') {
                rest = parent;
                let Some(id) = self.exact(parent, symbol_map, caller_file, false)? else {
                    continue;
                };
                let kind: Option<String> = self
                    .conn
                    .query_row("SELECT kind FROM symbols WHERE id = ?", [id], |r| r.get(0))
                    .optional()?;
                if matches!(kind.as_deref(), Some("const" | "variable")) {
                    hit = Some(id);
                }
                break;
            }
            let Some(id) = hit else { continue };
            match found {
                None => found = Some(id),
                Some(existing) if existing == id => {}
                Some(_) => {
                    self.saw_ambiguous = true;
                    return Ok(None);
                }
            }
        }
        Ok(found)
    }

    /// Resolve an `IMPORTS_FILE` edge's ordered candidate list (see
    /// `EdgeInput::import_candidates` / `python::resolve_import_file_edges`)
    /// to the first candidate naming exactly one `module`-kind symbol.
    /// Unlike `resolve_import` above (unique across *every* candidate),
    /// this is first-hit: the extractor orders candidates from most to
    /// least specific (a `from pkg import mod` import's `pkg.mod`
    /// submodule guess before its `pkg` package fallback), so once the
    /// more specific module exists it must win over an already-bound
    /// fallback, not merely disambiguate an otherwise-tied name.
    fn resolve_import_file(&mut self, candidates: &[String]) -> Result<Option<i64>> {
        let gv = self.graph_version;
        let was_ambiguous = std::mem::take(&mut self.saw_ambiguous);
        for candidate in candidates {
            if let Some(id) = self.unique(Lookup::Module, params![candidate, gv])? {
                self.saw_ambiguous = was_ambiguous;
                return Ok(Some(id));
            }
            // An ambiguous candidate stops the walk: falling through to a
            // less specific one would be a guess.
            if self.saw_ambiguous {
                return Ok(None);
            }
        }
        self.saw_ambiguous = was_ambiguous;
        Ok(None)
    }

    /// Whether a Python edge's unresolved import candidates point into this
    /// repo: a relative import (`.mod.x`, leading dot), or one whose root
    /// package has a Python `module` symbol here. When false, the import is
    /// external (stdlib, third-party) and refuses the name-based tiers.
    ///
    /// ponytail: root-name only, so a repo submodule sharing a stdlib root
    /// name (`pkg.common.logging` vs `import logging`) makes that stdlib
    /// import look repo-local and keeps the name-based tiers for it.
    /// Upgrade path: match the import's full module path, not just its root.
    ///
    /// ponytail: a `_pb2`/`_pb2_grpc` segment is protoc output, never
    /// checked in, so it counts as external even under a repo package —
    /// otherwise `pb.ColumnDef(...)` from `from pkg.v1 import pkg_pb2 as pb`
    /// binds to a same-named repo dataclass (41 such edges in dpb). Other
    /// gitignored generated modules still slip through; upgrade path: check
    /// the repo module's own bindings for the next segment.
    fn is_repo_python_import(&mut self, candidates: &[String]) -> Result<bool> {
        for candidate in candidates {
            if candidate
                .split('.')
                .any(|seg| seg.ends_with("_pb2") || seg.ends_with("_pb2_grpc"))
            {
                continue;
            }
            let root = candidate.split('.').next().unwrap_or("");
            if root.is_empty() || self.repo_python_module.exists(params![root])? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Tier 6 (issue #80), the known-external fallback -- see the module
    /// doc. Called only once every earlier tier has found nothing at all
    /// for `target_qualname`. Rust and Go are the only languages that need
    /// it: neither ever populates `import_candidates` for a qualified call
    /// (`rust::import_qualified_candidates` explicitly skips any raw text
    /// containing `::`; Go's extractor doesn't track imports at all), so
    /// `refuse_names` in `resolve` never has anything to refuse on for
    /// them -- every other language's known-external signal already comes
    /// from tier 3. Not a `LanguageProfile` field: Go's check needs a
    /// database lookup (`is_repo_go_package`), which a plain per-language
    /// function pointer can't carry.
    fn is_known_external_fallback(
        &mut self,
        source_lang: &str,
        target_qualname: &str,
    ) -> Result<bool> {
        match resolution_language_family(source_lang) {
            // Every repo qualname in this codebase is rooted at `crate::`
            // (`rust::PROFILE`'s separator, and every rewrite in
            // `rust::resolve_call_target` produces a `crate::`-rooted
            // result) -- a `::`-qualified call target that isn't is
            // unambiguously a fully-qualified path into `std`/`core` or a
            // third-party crate the local tiers above already had their
            // normal shot at (a bare or two-segment call is always
            // module-qualified with a `crate::` prefix before reaching
            // here, so this never second-guesses one of those).
            "rust" => Ok(is_foreign_rust_path(target_qualname)),
            // A real Go qualname is always `<dir>/<file-stem>.<name>` (see
            // `VisibilityRule::GoCapitalization`'s doc); a package-qualified
            // call keeps its literal `pkg.Name` call-site text with no `/`
            // at all (`go::resolve_call_target`). `is_repo_go_package`
            // checks the package segment against real indexed directories
            // rather than assuming "no `/`" alone means external, so a
            // same-repo cross-package call the name tiers can't bind yet
            // (a known, separate gap -- see the golden Go fixture's `#
            // xfail: pkg.Func calls are not bound through the import path`
            // lines) stays a plain miss instead of a wrong external label.
            //
            // ponytail: Go tracks no receiver-type signal at all, so a
            // local-variable method call (`x.Method()`) is syntactically
            // identical to a package call here. One that fails to bind for
            // any reason (not just a genuine external call) and whose
            // one-letter-ish receiver name never collides with a real
            // directory can still be mislabeled external. Upgrade path:
            // track Go imports and locals the way Python/C#/JS-TS do.
            "go" => match target_qualname.split_once('.') {
                Some((package, _)) => Ok(!self.is_repo_go_package(package)?),
                None => Ok(false),
            },
            _ => Ok(false),
        }
    }

    /// Whether `package` (a Go call target's leading segment) is a
    /// directory or root-level file this graph_version actually indexes --
    /// see `is_known_external_fallback`'s doc.
    fn is_repo_go_package(&mut self, package: &str) -> Result<bool> {
        let gv = self.graph_version;
        Ok(self
            .conn
            .query_row(
                "SELECT 1 FROM files
             WHERE language = 'go'
               AND (deleted_version IS NULL OR deleted_version > ?1)
               AND (path LIKE ?2 || '/%' OR path = ?2 || '.go' OR path LIKE '%/' || ?2 || '.go')
             LIMIT 1",
                params![gv, package],
                |_| Ok(()),
            )
            .optional()?
            .is_some())
    }

    /// Tier 3/6's shared "bind to the external stub" outcome (issue #80).
    /// Builds the stub's qualname from the reference's own text and
    /// get-or-creates its symbol row (`resolve_external_stub`) -- or, when
    /// there's no text to build one from at all (e.g. a Python method call
    /// through a receiver that's itself a call result, `ReceiverType::
    /// Unresolved` with no import binding involved: neither `target_qualname`
    /// nor `import_candidates` has anything), falls back to a plain
    /// `NoCandidates` miss rather than creating a meaningless, shared
    /// `ext:?` stub every such call site would collapse into. This matches
    /// what happened here before #80 anyway: `store_reference_name_and_tail`
    /// already refuses to store an unresolved reference with no name to key
    /// on, so a text-less reference was already silently dropped, never an
    /// edge or a store row.
    ///
    /// `via_language_fallback` is threaded straight into
    /// `ResolutionKind::External` -- see its doc and
    /// `Resolution::stored_receiver_type`.
    fn stub_resolution(
        &mut self,
        source_lang: &str,
        target_qualname: Option<&str>,
        import_candidates: &[String],
        bare_call: bool,
        via_language_fallback: bool,
    ) -> Result<Option<Resolution>> {
        let Some(qualname) = external_stub_qualname(target_qualname, import_candidates, bare_call)
        else {
            return Ok(Some(Resolution::Unresolved(UnresolvedReason::NoCandidates)));
        };
        if !self.denotes_external_symbol(source_lang, &qualname, import_candidates)? {
            return Ok(None);
        }
        let stub_id = self.resolve_external_stub(&qualname)?;
        Ok(Some(resolved(
            stub_id,
            ResolutionKind::External {
                via_language_fallback,
            },
        )))
    }

    /// Whether the stub name `qualname` (`ext:`-prefixed) can denote a real
    /// entity outside the repository (issue #256). It cannot when it is
    /// raw call-expression text (`a.b().c`) or names an entity this
    /// repository declares or a member of one of its types. Nor can a name that is only the
    /// call's own receiver-plus-member text -- no import candidate ends
    /// with it, so it names a local value, not an import-bound entity --
    /// when every import candidate points into the repository too: the
    /// evidence then says the target is a repository symbol that is gone.
    fn denotes_external_symbol(
        &mut self,
        source_lang: &str,
        qualname: &str,
        import_candidates: &[String],
    ) -> Result<bool> {
        let name = qualname
            .strip_prefix(EXTERNAL_STUB_PREFIX)
            .unwrap_or(qualname);
        if is_raw_call_text(name) || self.names_repo_entity(source_lang, name)? {
            return Ok(false);
        }
        if import_candidates.is_empty() || import_candidates.iter().any(|c| ends_with_name(c, name))
        {
            return Ok(true);
        }
        for candidate in import_candidates {
            if !self.names_repo_entity(source_lang, candidate)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Whether `name` points at an entity this graph_version declares in
    /// the language family of `source_lang`: a symbol with exactly that
    /// qualname, or a member of a repository type (its parent qualname is a
    /// non-namespace, non-module symbol), which means the member is gone.
    /// Merely sharing a namespace or module prefix with the repository does
    /// not count -- a repo may declare `namespace Microsoft.Extensions...`
    /// or a root `utils.ts` and still call the external package of that
    /// name. Memoised per resolver.
    fn names_repo_entity(&mut self, source_lang: &str, name: &str) -> Result<bool> {
        let family = resolution_language_family(source_lang);
        let key = (family.to_string(), name.to_string());
        if let Some(hit) = self.repo_entity_memo.get(&key) {
            return Ok(*hit);
        }
        let parent = qualname_prefixes(name)
            .into_iter()
            .rev()
            .nth(1)
            .map(str::to_string);
        let mut stmt = self.conn.prepare_cached(
            "SELECT f.language, s.kind FROM symbols s JOIN files f ON f.id = s.file_id
             WHERE s.graph_version = ?1 AND s.qualname = ?2 AND s.kind <> 'external'",
        )?;
        let mut found = false;
        let mut lookup = |qualname: &str, member_of: bool| -> Result<bool> {
            let mut rows = stmt.query(params![self.graph_version, qualname])?;
            while let Some(row) = rows.next()? {
                let language: String = row.get(0)?;
                let kind: String = row.get(1)?;
                if resolution_language_family(&language) == family
                    && (!member_of || !matches!(kind.as_str(), "namespace" | "module"))
                {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        if lookup(name, false)? {
            found = true;
        } else if let Some(parent) = parent {
            found = lookup(&parent, true)?;
        }
        self.repo_entity_memo.insert(key, found);
        Ok(found)
    }

    /// Get-or-create the external stub symbol for `qualname` (already
    /// `ext:`-prefixed) in this resolver's `graph_version` -- issue #80.
    /// One stub per `(graph_version, qualname)`, enforced by the partial
    /// unique index on `symbols(graph_version, qualname) WHERE kind =
    /// 'external'` (schema v20) and reused across every call site sharing
    /// it: the `SELECT` below is single-threaded-safe because indexing
    /// never resolves references concurrently, but the `INSERT ... ON
    /// CONFLICT DO NOTHING` plus a second `SELECT` is used anyway so this
    /// stays correct even if that ever changes, without relying on
    /// `last_insert_rowid` (which an ignored conflict wouldn't advance).
    fn resolve_external_stub(&mut self, qualname: &str) -> Result<i64> {
        let gv = self.graph_version;
        if let Some(id) = self.external_stub_id(qualname)? {
            return Ok(id);
        }
        let file_id = self.external_file_id()?;
        let name = qualname_trailing_name(qualname);
        let stable_id = format!("external:{qualname}");
        self.conn.execute(
            "INSERT INTO symbols
                (file_id, kind, name, qualname, start_line, start_col, end_line, end_col,
                 start_byte, end_byte, graph_version, stable_id)
             VALUES (?1, 'external', ?2, ?3, 0, 0, 0, 0, 0, 0, ?4, ?5)
             ON CONFLICT DO NOTHING",
            params![file_id, name, qualname, gv, stable_id],
        )?;
        self.external_stub_id(qualname)?
            .ok_or_else(|| anyhow::anyhow!("external stub symbol missing after insert: {qualname}"))
    }

    fn external_stub_id(&self, qualname: &str) -> Result<Option<i64>> {
        Ok(self
            .conn
            .query_row(
                "SELECT id FROM symbols WHERE graph_version = ?1 AND qualname = ?2 AND kind = 'external'",
                params![self.graph_version, qualname],
                |row| row.get(0),
            )
            .optional()?)
    }

    /// The `files.id` of the single synthetic pseudo-file every external
    /// stub symbol belongs to (issue #80) -- `symbols.file_id` is `NOT
    /// NULL`, and a stub has no real source file, so one shared row (path
    /// `<external>`, language `external`) stands in for it, created lazily
    /// on first use and reused for the life of the database (not
    /// versioned like a real file -- stub *symbols* are versioned by their
    /// own `graph_version` column the same as any other symbol; this file
    /// row is just their common parent). `f.language = 'external'` is what
    /// repo-internal listings (`module_summary`, `module_edges`,
    /// `count_symbols_by_kind`) filter on to keep stubs out.
    fn external_file_id(&mut self) -> Result<i64> {
        if let Some(id) = self.external_file_id {
            return Ok(id);
        }
        self.conn.execute(
            "INSERT OR IGNORE INTO files (path, hash, language, size, modified)
             VALUES (?1, '', 'external', 0, 0)",
            params![EXTERNAL_FILE_PATH],
        )?;
        let id: i64 = self.conn.query_row(
            "SELECT id FROM files WHERE path = ?1",
            params![EXTERNAL_FILE_PATH],
            |row| row.get(0),
        )?;
        self.external_file_id = Some(id);
        Ok(id)
    }
}

/// The single synthetic file every external stub symbol belongs to --
/// see `Resolver::external_file_id`.
pub(crate) const EXTERNAL_FILE_PATH: &str = "<external>";

/// C# `Type.Member` reads (the only C# `USES` edges, issue #247) are
/// speculative: most hit framework members (`DateTime.UtcNow`), so binding
/// them to `ext:` stubs is pure noise. They stay unresolved (and retryable)
/// until an in-repo target appears.
fn never_external(r: &Reference<'_>) -> bool {
    r.edge_kind == "USES" && r.source_lang == "csharp"
}

/// The `ext:` marker every external stub qualname starts with.
const EXTERNAL_STUB_PREFIX: &str = "ext:";

/// Raw call-expression text (`a.b().c`, `x[0].y`) rather than a dotted
/// name: never a real external entity's name.
fn is_raw_call_text(name: &str) -> bool {
    name.chars().any(|c| {
        c.is_whitespace() || matches!(c, '(' | ')' | '[' | ']' | '{' | '}' | ';' | '"' | '\'')
    })
}

/// Whether `candidate` is `name` or ends with it at a segment boundary
/// (`.`, `::` or the `:` JS/TS candidates use after the module).
fn ends_with_name(candidate: &str, name: &str) -> bool {
    candidate
        .strip_suffix(name)
        .is_some_and(|head| head.is_empty() || head.ends_with(['.', ':']))
}

/// `name` and each leading prefix of it that ends before a `.` or `::`
/// separator.
fn qualname_prefixes(name: &str) -> Vec<&str> {
    let bytes = name.as_bytes();
    let mut out = Vec::new();
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (*b == b'.' || (*b == b':' && bytes.get(i + 1) == Some(&b':'))) {
            out.push(&name[..i]);
        }
    }
    out.push(name);
    out
}

/// The stub qualname for a known-external outcome (issue #80): `ext:` plus
/// the reference's own text.
///
/// For a *bare* call (`bare_call`, e.g. `entry()`) whose import tier missed,
/// `target_qualname` is a same-module guess a bare call always gets qualified
/// with regardless of where it's actually bound (e.g. Python's
/// `resolve_call_target` turns `entry()` into `<this file's own module>.entry`
/// even when it's really `from caller import entry`) -- actively misleading
/// as this reference's identity once an import candidate names what it's
/// bound to instead, so the first import candidate wins here. Every other
/// shape (an attribute/qualified call, or tier 6's Rust/Go fallback, which
/// never has import candidates to prefer) keeps `target_qualname`, its own
/// literal, authoritative call-site text.
///
/// `None` when neither `target_qualname` nor `import_candidates` has any
/// text at all -- reachable when `receiver_type` is a directly
/// extractor-reported `Some("")` with no import involved (e.g. Python's
/// `ReceiverType::Unresolved` for a method call through a receiver that's
/// itself a call result, whose raw call-site text fails
/// `is_simple_call_target` and whose dotted-method guard in
/// `import_qualified_candidates` also refuses) -- see `stub_resolution`'s
/// doc for why that case must not fall through to here regardless.
fn external_stub_qualname(
    target_qualname: Option<&str>,
    import_candidates: &[String],
    bare_call: bool,
) -> Option<String> {
    let first_candidate = || import_candidates.first().map(String::as_str);
    let text = if bare_call {
        first_candidate().or(target_qualname)
    } else {
        target_qualname.or_else(first_candidate)
    }?;
    Some(format!("{EXTERNAL_STUB_PREFIX}{text}"))
}

fn resolved(target_id: i64, kind: ResolutionKind) -> Resolution {
    Resolution::Resolved { target_id, kind }
}

/// The shared `INSERT INTO unresolved_references` statement (issue #78/#79):
/// one row per `Resolver::resolve` `Unresolved` outcome, so
/// `Db::retry_unresolved_references` can retry it later without rescanning
/// every edge. The store is self-contained (issue #79): `edge_id` is
/// `Some` only for a Bridge Edge kind, which keeps its placeholder edge in
/// `edges` regardless of resolution (see `Db::insert_edges`'s doc); every
/// other kind has no edge at all while unresolved; a retry success rebuilds
/// one from this row's own shadow columns instead of updating an existing
/// one. Column/param order: edge_id, source_symbol_id, file_id, edge_kind,
/// reference_name, name_tail, reason, import_candidates, detail,
/// evidence_snippet, evidence_start_line, evidence_end_line, confidence,
/// commit_sha, trace_id, span_id, event_ts, receiver_type, bare_call,
/// call_shape, graph_version, receiver_scope, deferred_kind, deferred. A
/// pending reference (`edge_id` NULL) already stored for the same identity
/// (migration 27's partial unique index) is kept, not duplicated (issue
/// #251). Used by `Db::insert_edges` (an edge's first resolution attempt), `reconcile_unresolved_reference_store` (an edge that went
/// NULL-target with no row yet), and `Db::carry_forward_references` (carrying an
/// already-unresolved reference into the new graph version).
pub(crate) const UNRESOLVED_REFERENCE_INSERT_SQL: &str = "INSERT INTO unresolved_references
     (edge_id, source_symbol_id, file_id, edge_kind, reference_name, name_tail,
      reason, import_candidates, detail, evidence_snippet, evidence_start_line,
      evidence_end_line, confidence, commit_sha, trace_id, span_id, event_ts,
      receiver_type, bare_call, call_shape, graph_version, receiver_scope, deferred_kind,
      deferred)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT DO NOTHING";

/// Everything `Resolver::resolve` needs to re-judge one reference, shared by
/// `NullTargetEdgeRow` (read straight from an edge with no store row yet)
/// and `StoreRetryRow` (read from a stored `unresolved_references` row
/// instead) — the two repair-pass row shapes used to each carry their own
/// copy of these same fields and rebuild an identical `Reference` from them.
/// Each row type's own extra bookkeeping field stays outside this struct:
/// `NullTargetEdgeRow`'s `source_symbol_id`/`file_id` (for a fresh store
/// insert if still unresolved) and `StoreRetryRow`'s `store_id` (to delete
/// on success) plus both rows' shadow columns (for rebuilding an edge on a
/// successful retry -- see `UNRESOLVED_REFERENCE_INSERT_SQL`'s doc).
///
/// `edge_id` is `None` for a pending, non-Bridge-Edge-kind reference (no
/// edge exists yet -- issue #79): `NullTargetEdgeRow` always has `Some`
/// (built from a live edge), `StoreRetryRow` carries whatever its stored
/// row has.
struct ReferenceContext {
    edge_id: Option<i64>,
    target_qualname: Option<String>,
    edge_kind: String,
    receiver_type: Option<String>,
    receiver_scope: Option<String>,
    deferred_kind: Option<String>,
    deferred: Option<String>,
    import_candidates: Option<String>,
    bare_call: bool,
    call_shape: Option<String>,
    source_lang: String,
    file_path: String,
    source_qualname: Option<String>,
    source_symbol_id: Option<i64>,
}

impl ReferenceContext {
    /// The stored deferred marker, if any.
    fn marker(&self) -> Option<DeferredMarker> {
        DeferredMarker::decode(self.deferred_kind.as_deref()?, self.deferred.as_deref()?)
    }

    /// The callee text this reference keeps as its `target_qualname` while a
    /// deferred-argument edge is unbound (`None` for any other reference).
    fn deferred_callee(&self) -> Option<String> {
        match self.marker()? {
            DeferredMarker::Argument(arg) => Some(arg.callee),
            _ => None,
        }
    }

    /// Rebuild this context's `Reference` and run it through `resolver`.
    fn resolve(
        &self,
        resolver: &mut Resolver<'_>,
        symbol_map: &HashMap<String, i64>,
    ) -> Result<Resolution> {
        let import_candidates = self
            .import_candidates
            .as_deref()
            .map(decode_import_candidates)
            .unwrap_or_default();
        let marker = self.marker();
        let scope = self
            .receiver_scope
            .as_deref()
            .map(|column| TypeScope::decode(Some(column)));
        resolver.resolve(
            &Reference {
                target_qualname: self.target_qualname.as_deref(),
                edge_kind: &self.edge_kind,
                receiver_type: self.receiver_type.as_deref(),
                receiver_scope: scope.as_ref(),
                deferred: marker.as_ref(),
                import_candidates: &import_candidates,
                source_lang: &self.source_lang,
                source_file_path: &self.file_path,
                source_qualname: self.source_qualname.as_deref(),
                source_symbol_id: self.source_symbol_id,
                bare_call: self.bare_call,
                call_shape: self.call_shape.as_deref().and_then(CallShape::decode),
            },
            symbol_map,
        )
    }
}

/// The `target_qualname` to store for an edge just bound to `target_id`: the
/// bound symbol's qualname for a deferred-argument edge (whose own text is
/// only its callee's name), else the extracted text unchanged.
pub(crate) fn bound_target_qualname(
    conn: &Connection,
    is_deferred_argument: bool,
    target_qualname: Option<&str>,
    target_id: i64,
) -> Result<Option<String>> {
    if is_deferred_argument {
        return Ok(conn
            .query_row(
                "SELECT qualname FROM symbols WHERE id = ?",
                params![target_id],
                |row| row.get(0),
            )
            .optional()?);
    }
    Ok(target_qualname.map(str::to_string))
}

/// Rebinds an edge (`?1` target id or NULL, `?2` resolution kind, `?3` edge
/// id, `?4` the callee text). A deferred-argument edge also keeps its
/// `target_qualname` in step: the bound constructor's qualname, or the
/// callee's name again once unbound.
static UPDATE_EDGE_TARGET_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "UPDATE edges SET target_symbol_id = ?1, resolution_kind = ?2,
           target_qualname = CASE WHEN deferred_kind = '{DEFERRED_KIND_ARGUMENT}'
             THEN COALESCE((SELECT qualname FROM symbols WHERE id = ?1), ?4)
             ELSE target_qualname END
         WHERE id = ?3"
    )
});

/// The in-batch fast path `Resolver::exact` (and `resolve_import`, which
/// calls it) consult before falling through to `EXACT_SQL` — every symbol
/// just written for one file, keyed by qualname, collapsed through
/// `collapse_exact_candidates`'s own rule (all same-file here by
/// construction, so only its kind check does anything: issue #77's
/// `@overload`/overload-signature case). A qualname whose same-file
/// symbols differ in kind (e.g. `class g` + `def g`) is left out of the
/// map entirely, so the SQL fallback re-judges it the same way.
pub(crate) fn build_exact_symbol_map(symbols: &[crate::model::Symbol]) -> HashMap<String, i64> {
    let mut by_qualname: HashMap<&str, Vec<(i64, &str)>> = HashMap::new();
    for symbol in symbols {
        // Every symbol here already comes from the one file this batch just
        // wrote, so there's no file to compare (unlike
        // `collapse_exact_candidates`'s cross-file rows) — `same_kind_min`
        // alone is this fast path's whole rule.
        by_qualname
            .entry(symbol.qualname.as_str())
            .or_default()
            .push((symbol.id, symbol.kind.as_str()));
    }
    let mut map = HashMap::with_capacity(by_qualname.len());
    for (qualname, candidates) in by_qualname {
        // A shared qualname (overloads, issue #123) is left to the SQL
        // fallback, where a call's arity can pick between them.
        if let [(id, _)] = candidates[..] {
            map.insert(qualname.to_string(), id);
        }
    }
    map
}

/// Collapses `candidates` to the lowest id when every one shares the same
/// `kind` (an overload set) — `None` for an empty slice or a kind
/// mismatch. Shared by `build_exact_symbol_map` (no file to compare) and
/// `collapse_exact_candidates` (file check applied separately, first).
fn same_kind_min(candidates: &[(i64, &str)]) -> Option<i64> {
    let (_, first_kind) = candidates.first()?;
    candidates
        .iter()
        .all(|(_, kind)| kind == first_kind)
        // `candidates` is non-empty here (`first()?` above already
        // returned for an empty slice), so `.min()` always has a row.
        .then(|| candidates.iter().map(|(id, _)| *id).min().unwrap())
}

/// Issue #77's ambiguity rule for a qualname naming more than one symbol
/// (`candidates` as `(id, file_id, kind)`, `EXACT_SQL`'s row shape):
/// collapse to the lowest id when every candidate shares both a file and a
/// kind (overloads) — the same symbol a base, non-ambiguous full reindex
/// already binds to. Any other shape (different files, or different kinds,
/// e.g. `class g` + `def g`) is genuinely ambiguous: `None`, never a guess.
/// Used by `Resolver::exact`'s SQL fallback.
fn collapse_exact_candidates(candidates: &[ExactCandidate]) -> Option<i64> {
    let first_file = candidates.first()?.file_id;
    if !candidates.iter().all(|c| c.file_id == first_file) {
        return canonical_multi_file(candidates.iter());
    }
    let by_kind: Vec<(i64, &str)> = candidates.iter().map(|c| (c.id, c.kind.as_str())).collect();
    same_kind_min(&by_kind)
}

/// The kinds a C# `partial` declaration can have (each can be split across
/// files, so each is a candidate "one entity, many parts" kind).
const PARTIAL_TYPE_KINDS: [&str; 4] = ["class", "struct", "record", "interface"];

/// Issue #206: candidates in 2+ files that are parts of one entity rather
/// than competing symbols -- every candidate a `namespace` (declared per
/// file, defined to span files), or every one a type of one kind that is
/// itself declared `partial`. Resolves to the part in the lexicographically
/// first file (then lowest id), a canonical choice that depends on the tree,
/// never on scan order. `None` for anything else: same-named but unrelated
/// symbols (including a non-`partial` twin of a `partial` type) stay
/// ambiguous.
fn canonical_multi_file<'a>(
    candidates: impl Iterator<Item = &'a ExactCandidate> + Clone,
) -> Option<i64> {
    let first = candidates.clone().next()?;
    if candidates.clone().any(|c| c.kind != first.kind) {
        return None;
    }
    let one_entity = match first.kind.as_str() {
        "namespace" => true,
        kind if PARTIAL_TYPE_KINDS.contains(&kind) => candidates
            .clone()
            .all(|c| is_partial_signature(c.signature.as_deref())),
        _ => false,
    };
    if !one_entity {
        return None;
    }
    candidates
        .min_by(|a, b| (&a.path, a.id).cmp(&(&b.path, b.id)))
        .map(|c| c.id)
}

/// One `EXACT_SQL` row.
struct ExactCandidate {
    id: i64,
    file_id: i64,
    path: String,
    kind: String,
    signature: Option<String>,
}

/// A call's argument count, for choosing among same-qualname overloads.
/// `value_receiver` is true when the call's receiver is a value rather than
/// a type name or nothing (`kind.ToDb()`), which is the only form an
/// extension method's `this` parameter is filled implicitly.
#[derive(Clone, Copy)]
struct Arity {
    args: usize,
    value_receiver: bool,
}

/// Whether a method with this indexed signature can take `args` arguments
/// (a language hook's view of `arity_admits`, without an extension receiver).
pub(crate) fn admits_arg_count(args: usize, signature: &str) -> bool {
    arity_admits(
        Some(Arity {
            args,
            value_receiver: false,
        }),
        "method",
        Some(signature),
    )
}

/// Whether a candidate symbol can take a call of `arity`. Always true when
/// there's no arity signal, for a non-callable kind, or for a signature this
/// can't read -- only a callable positively known not to fit is excluded.
fn arity_admits(arity: Option<Arity>, kind: &str, signature: Option<&str>) -> bool {
    let (Some(arity), true, Some(signature)) = (arity, kind == "method", signature) else {
        return true;
    };
    let Some(params) = signature.strip_prefix('(').and_then(parameter_list) else {
        return true;
    };
    let mut required = 0;
    let mut max = Some(0usize);
    let mut is_extension = false;
    for (i, param) in split_top_level(params).into_iter().enumerate() {
        let param = param.trim();
        if param.is_empty() {
            continue;
        }
        is_extension |= i == 0 && param.starts_with("this ");
        if param.starts_with("params ") {
            max = None;
        } else if has_top_level_default(param) {
            max = max.map(|m| m + 1);
        } else {
            required += 1;
            max = max.map(|m| m + 1);
        }
    }
    let fits = |n: usize| n >= required && max.is_none_or(|m| n <= m);
    let direct = fits(arity.args);
    let extension = is_extension && fits(arity.args + 1);
    match (is_extension, arity.value_receiver) {
        (true, true) => extension,
        (true, false) => direct || extension,
        _ => direct,
    }
}

/// The characters of `s` outside any `()`/`<>`/`[]`/`{}` nesting, with their
/// byte offsets. A closer with nothing open is yielded (it ends the
/// enclosing list), as is an opener itself.
pub(crate) fn top_level_chars(s: &str) -> impl Iterator<Item = (usize, char)> + '_ {
    let mut depth = 0usize;
    s.char_indices().filter(move |&(_, c)| {
        let at_top = depth == 0;
        match c {
            '(' | '<' | '[' | '{' => depth += 1,
            ')' | '>' | ']' | '}' => depth = depth.saturating_sub(1),
            _ => {}
        }
        at_top
    })
}

/// The text between a signature's opening `(` (already stripped) and its
/// matching `)`.
pub(crate) fn parameter_list(rest: &str) -> Option<&str> {
    let (end, _) = top_level_chars(rest).find(|&(_, c)| c == ')')?;
    Some(&rest[..end])
}

pub(crate) fn split_top_level(params: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    for (i, _) in top_level_chars(params).filter(|&(_, c)| c == ',') {
        parts.push(&params[start..i]);
        start = i + 1;
    }
    parts.push(&params[start..]);
    parts
}

fn has_top_level_default(param: &str) -> bool {
    top_level_chars(param).any(|(_, c)| c == '=')
}

/// The simple type name (no namespace, generics or `?`) of an extension
/// method signature's `this` parameter, e.g. `(this Ns.Kind? k) -> string`
/// gives `Kind`. `None` for a signature that isn't an extension method's.
fn extension_receiver_type(signature: &str) -> Option<&str> {
    let params = parameter_list(signature.strip_prefix('(')?)?;
    let first = split_top_level(params).into_iter().next()?;
    let (ty, _name) = first
        .trim()
        .strip_prefix("this ")?
        .trim()
        .rsplit_once(' ')?;
    Some(simple_type_name(ty))
}

/// `Ns.List<int>?` -> `List`.
fn simple_type_name(ty: &str) -> &str {
    let ty = ty.trim().trim_end_matches('?');
    let ty = ty.split('<').next().unwrap_or(ty);
    ty.rsplit('.').next().unwrap_or(ty).trim()
}

/// `Some(only)` when `items` yields exactly one item.
fn exactly_one<T>(mut items: impl Iterator<Item = T>) -> Option<T> {
    let first = items.next()?;
    items.next().is_none().then_some(first)
}

/// Runs `EXACT_SQL` (or an equivalent prepared statement) for `qualname`,
/// collecting every `ExactCandidate` row (fixture-path rows from outside
/// fixtures dropped) for `collapse_exact_candidates` or the arity filter
/// to judge.
fn query_exact_candidates(
    stmt: &mut Statement<'_>,
    qualname: &str,
    graph_version: i64,
    caller_file: &str,
) -> Result<Vec<ExactCandidate>> {
    let rows = stmt.query_map(params![qualname, graph_version], |row| {
        Ok((
            ExactCandidate {
                id: row.get(0)?,
                file_id: row.get(1)?,
                path: row.get(3)?,
                kind: row.get(2)?,
                signature: row.get(4)?,
            },
            row.get::<_, String>(3)?,
        ))
    })?;
    let mut candidates = Vec::new();
    for row in rows {
        let (candidate, path) = row?;
        // Issue #102: fixture symbols are never targets from outside them.
        if is_fixture_path(&path) && !is_fixture_path(caller_file) {
            continue;
        }
        candidates.push(candidate);
    }
    Ok(candidates)
}

impl Db {
    /// Issue #77: clears `target_symbol_id`/`resolution_kind` on every
    /// cross-file edge bound to a symbol named by one of `qualnames` (the
    /// qualnames a sync batch just added) — whether that's the edge's own
    /// stored `target_qualname` text, or its already-resolved target's
    /// qualname (a caller-module placeholder, e.g. an import-bound bare
    /// call, never matches by text). Excludes a same-file edge: that
    /// binding came from `build_exact_symbol_map`'s in-batch fast path,
    /// which never sees other files, so a same-qualname symbol elsewhere
    /// can't make it ambiguous. Call before
    /// `reconcile_unresolved_reference_store`, which re-judges the cleared
    /// rows (they have no store row of their own yet) under its ambiguity
    /// rule.
    pub(crate) fn unbind_edges_for_qualnames(
        &self,
        qualnames: &std::collections::HashSet<String>,
        graph_version: i64,
    ) -> Result<usize> {
        if qualnames.is_empty() {
            return Ok(0);
        }
        let placeholders = vec!["?"; qualnames.len()].join(",");
        let sql = format!(
            "UPDATE edges SET target_symbol_id = NULL, resolution_kind = NULL
             WHERE graph_version = ? AND target_symbol_id IS NOT NULL
               AND (
                 target_qualname IN ({placeholders})
                 OR (SELECT s.qualname FROM symbols s WHERE s.id = edges.target_symbol_id)
                     IN ({placeholders})
               )
               AND file_id != (SELECT s.file_id FROM symbols s WHERE s.id = edges.target_symbol_id)"
        );
        let conn = self.conn();
        let mut stmt = conn.prepare(&sql)?;
        let mut p: Vec<Box<dyn ToSql>> = vec![Box::new(graph_version)];
        p.extend(
            qualnames
                .iter()
                .map(|q| Box::new(q.clone()) as Box<dyn ToSql>),
        );
        p.extend(
            qualnames
                .iter()
                .map(|q| Box::new(q.clone()) as Box<dyn ToSql>),
        );
        Ok(stmt.execute(rusqlite::params_from_iter(p.iter().map(|b| b.as_ref())))?)
    }

    /// Issue #78/#79: give a store row to every NULL-target edge that has
    /// no `unresolved_references` row -- because it never got one in the
    /// first place. Two ways that happens:
    ///
    /// - `Db::carry_forward_references` copies an unchanged file's edges into
    ///   the new graph version, but not their store rows, so a
    ///   carried-forward edge that was already unresolved arrives with no
    ///   row to match it.
    /// - An edge that resolved cleanly when `Db::insert_edges` first ran it
    ///   (so no row was ever written) can still go NULL-target afterward --
    ///   the `edges` foreign key's `ON DELETE SET NULL` when its target is
    ///   deleted or renamed away, or `Db::unbind_edges_for_qualnames`
    ///   clearing it for re-judgment. Either way `target_symbol_id` goes
    ///   NULL but `resolution_kind` is left stale (neither of those two
    ///   writers touches it).
    ///
    /// For each such edge, rebuilds the same `Reference` context
    /// `retry_unresolved_references` reconstructs from a stored row --
    /// here read straight from the edge and its file/source-symbol joins
    /// instead -- and re-runs `Resolver::resolve`:
    ///
    /// - Resolved: the edge (which still exists either way) is updated in
    ///   place, same as `Db::insert_edges` would have done the first time.
    /// - Unresolved, Bridge Edge kind (issue #79): the edge keeps existing
    ///   (its target is a cross-language/cross-process join key, not
    ///   necessarily a symbol here -- see `Db::insert_edges`'s doc), just
    ///   with `resolution_kind` cleared (this module is the only producer
    ///   of that column, and this is now the one tidying it up); a store
    ///   row is added alongside it.
    /// - Unresolved, every other kind (issue #79): the edge is deleted --
    ///   it must not sit at rest with a NULL target -- and a self-contained
    ///   store row replaces it, built from the edge's own columns.
    ///
    /// Call before `retry_unresolved_references` at both repair sites, so a
    /// newly-orphaned edge gets a shot at every symbol that already exists
    /// before falling to a store row that retry's watermark-gated join
    /// would otherwise leave stranded until some unrelated symbol insertion
    /// came along.
    ///
    /// Returns the number of edges reconciled (resolved or newly stored).
    pub fn reconcile_unresolved_reference_store(&self, graph_version: i64) -> Result<usize> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut reconciled = 0;

        let rows: Vec<NullTargetEdgeRow> = {
            let mut stmt = tx.prepare(
                "SELECT e.id, e.source_symbol_id, e.file_id, e.kind, e.target_qualname,
                        e.receiver_type, e.import_candidates, e.bare_call,
                        e.detail, e.evidence_snippet, e.evidence_start_line, e.evidence_end_line,
                        e.confidence, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                        COALESCE(f.language, 'unknown'), f.path, src.qualname, e.call_shape,
                        e.receiver_scope, e.deferred_kind, e.deferred
                 FROM edges e
                 JOIN files f ON f.id = e.file_id
                 LEFT JOIN symbols src ON src.id = e.source_symbol_id
                 LEFT JOIN unresolved_references ur ON ur.edge_id = e.id
                 WHERE e.graph_version = ?
                   AND e.target_symbol_id IS NULL
                   AND ur.id IS NULL
                   AND (e.target_qualname IS NOT NULL OR e.import_candidates IS NOT NULL)",
            )?;
            let out = stmt.query_map(params![graph_version], |row| {
                Ok(NullTargetEdgeRow {
                    source_symbol_id: row.get(1)?,
                    file_id: row.get(2)?,
                    detail: row.get(8)?,
                    evidence_snippet: row.get(9)?,
                    evidence_start_line: row.get(10)?,
                    evidence_end_line: row.get(11)?,
                    confidence: row.get(12)?,
                    commit_sha: row.get(13)?,
                    trace_id: row.get(14)?,
                    span_id: row.get(15)?,
                    event_ts: row.get(16)?,
                    ctx: ReferenceContext {
                        edge_id: Some(row.get(0)?),
                        edge_kind: row.get(3)?,
                        target_qualname: row.get(4)?,
                        receiver_type: row.get(5)?,
                        receiver_scope: row.get(21)?,
                        deferred_kind: row.get(22)?,
                        deferred: row.get(23)?,
                        import_candidates: row.get(6)?,
                        bare_call: row.get(7)?,
                        call_shape: row.get(20)?,
                        source_lang: row.get(17)?,
                        file_path: row.get(18)?,
                        source_qualname: row.get(19)?,
                        source_symbol_id: row.get(1)?,
                    },
                })
            })?;
            out.collect::<rusqlite::Result<Vec<_>>>()?
        };

        {
            let mut resolver = Resolver::new(&tx, graph_version)?;
            let mut update_edge = tx.prepare(&UPDATE_EDGE_TARGET_SQL)?;
            let mut delete_edge = tx.prepare("DELETE FROM edges WHERE id = ?")?;
            let mut insert_unresolved = tx.prepare(UNRESOLVED_REFERENCE_INSERT_SQL)?;
            let mut clear_resolution_kind =
                tx.prepare("UPDATE edges SET resolution_kind = NULL WHERE id = ?")?;
            let empty_symbol_map: HashMap<String, i64> = HashMap::new();

            for row in &rows {
                // A live edge always has an id (this query only ever reads
                // from `edges`).
                let edge_id = row
                    .ctx
                    .edge_id
                    .expect("NullTargetEdgeRow always has an edge_id");
                let resolution = row.ctx.resolve(&mut resolver, &empty_symbol_map)?;
                match resolution {
                    Resolution::Resolved { target_id, kind } => {
                        update_edge.execute(params![
                            target_id,
                            kind.as_str(),
                            edge_id,
                            row.ctx.deferred_callee()
                        ])?;
                        reconciled += 1;
                    }
                    Resolution::Unresolved(reason) => {
                        let import_candidates = row
                            .ctx
                            .import_candidates
                            .as_deref()
                            .map(decode_import_candidates)
                            .unwrap_or_default();
                        let Some((reference_name, name_tail)) = store_reference_name_and_tail(
                            row.ctx.target_qualname.as_deref(),
                            &import_candidates,
                        ) else {
                            continue;
                        };
                        let is_bridge = is_bridge_edge_kind(&row.ctx.edge_kind);
                        let stored_edge_id = if is_bridge {
                            // `target_symbol_id` is already NULL (this
                            // row's selection criterion), but
                            // `resolution_kind` isn't this module's to
                            // leave stale -- see the doc.
                            clear_resolution_kind.execute(params![edge_id])?;
                            Some(edge_id)
                        } else {
                            delete_edge.execute(params![edge_id])?;
                            None
                        };
                        insert_unresolved.execute(params![
                            stored_edge_id,
                            row.source_symbol_id,
                            row.file_id,
                            &row.ctx.edge_kind,
                            reference_name,
                            name_tail,
                            reason.as_str(),
                            row.ctx.import_candidates.as_deref(),
                            row.detail.as_deref(),
                            row.evidence_snippet.as_deref(),
                            row.evidence_start_line,
                            row.evidence_end_line,
                            row.confidence,
                            row.commit_sha.as_deref(),
                            row.trace_id.as_deref(),
                            row.span_id.as_deref(),
                            row.event_ts,
                            row.ctx.receiver_type.as_deref(),
                            row.ctx.bare_call,
                            row.ctx.call_shape.as_deref(),
                            graph_version,
                            row.ctx.receiver_scope.as_deref(),
                            row.ctx.deferred_kind.as_deref(),
                            row.ctx.deferred.as_deref(),
                        ])?;
                        reconciled += 1;
                    }
                }
            }
        }

        tx.commit()?;
        Ok(reconciled)
    }

    /// Issue #78/#79: retry only `unresolved_references` rows whose
    /// reference name or trailing name segment (`name_tail`) matches a
    /// symbol inserted since the last call, instead of a rescan of every
    /// NULL-target edge. The `unresolved_reference_watermark` meta key
    /// tracks the highest `symbols.id` already considered, so a call with
    /// nothing new inserted is one cheap `MAX(id)` check.
    ///
    /// `symbols_deleted_this_batch` -- true when this sync/reindex removed
    /// any symbol, a whole file's or just one in-place-edited-away
    /// definition (see the callers' own docs) -- also retries every
    /// `reason = 'ambiguous'` or `'private'` row, matching a symbol or not,
    /// and skips the cheap-check early return so that pass still runs even
    /// when nothing new was inserted. Those are the only reasons a deletion
    /// (rather than an insertion) can change: they are caused by *too many*
    /// or only-private candidates, so removing one can make the reference
    /// unique again or leave no candidate at all, and no new `symbols.id`
    /// is ever inserted for the watermark to notice that by. Every other
    /// reason (`NoCandidates`, `External`) can only be fixed by something
    /// arriving, so those rows keep the normal watermark-gated join even on
    /// a deletion-carrying batch. Still only re-resolves rows already in the
    /// store, so this stays bounded by the store's size rather than every
    /// edge in the graph.
    ///
    /// Issue #78/#79 follow-up (finding G1): a reference resolvable only via
    /// the receiver-type/inheritance tier (`Resolver::resolve_via_inheritance`)
    /// can be unblocked by a brand-new EXTENDS/IMPLEMENTS/INHERITS edge on its
    /// receiver's type, without any symbol being inserted at all that shares
    /// the reference's own name/name_tail -- the ancestor that now makes it
    /// resolvable (e.g. `Base.bar`) can have existed since before the
    /// watermark; only the hierarchy edge is new. The
    /// `unresolved_reference_inheritance_watermark` meta key tracks the
    /// highest inheritance-kind `edges.id` already considered (same
    /// `MAX(id)` cheap-check shape as the symbol watermark, scoped to this
    /// `graph_version` so a same-version incremental sync only sees this
    /// version's own newly-written hierarchy edges); when it advances, every
    /// store row with a known receiver type (the only rows tier 4 could ever
    /// have produced) is retried regardless of name/name_tail, via a second,
    /// unjoined branch below. Still bounded by the store's size, not the
    /// whole edge table. On a fresh reindex's own repair pass this is
    /// necessarily true on the first check for that graph version (every
    /// carried/re-parsed inheritance edge gets a fresh id there), same cost
    /// class as `reconcile_unresolved_reference_store`'s own per-reindex pass.
    ///
    /// Each candidate is re-run through the *full* `Resolver::resolve` tier
    /// order (not just the tier that failed originally), using the
    /// reference's original context -- receiver type, import candidates,
    /// bare call, source language/file/caller qualname -- reconstructed
    /// from the stored row and its edge/file/symbol joins, identical to how
    /// `Db::insert_edges` resolved it the first time. A resolved reference
    /// updates its edge (`target_symbol_id`/`resolution_kind`) and leaves
    /// the store; a still-unresolved one is left in place for a later
    /// pass, relabelled with the reason the retry classified (a fresh parse
    /// of the same tree records that one, and `Ambiguous` rows are the ones
    /// a deletion revisits, so a stale label would strand the row).
    ///
    /// The join is a heuristic proxy for "worth retrying", not the
    /// resolver's own suffix-matching rule (`resolve_import`'s second
    /// round) -- a reference only a suffix match could satisfy, with no
    /// exact qualname or bare-name hit, can still be missed here.
    pub fn retry_unresolved_references(
        &self,
        graph_version: i64,
        symbols_deleted_this_batch: bool,
    ) -> Result<usize> {
        let watermark = self
            .get_meta_i64("unresolved_reference_watermark")?
            .unwrap_or(0);
        let max_symbol_id: i64 =
            self.read_conn()?
                .query_row("SELECT COALESCE(MAX(id), 0) FROM symbols", [], |row| {
                    row.get(0)
                })?;
        let inheritance_watermark = self
            .get_meta_i64("unresolved_reference_inheritance_watermark")?
            .unwrap_or(0);
        let max_inheritance_edge_id: i64 = self.read_conn()?.query_row(
            "SELECT COALESCE(MAX(id), 0) FROM edges
             WHERE graph_version = ? AND kind IN ('EXTENDS', 'IMPLEMENTS', 'INHERITS')",
            params![graph_version],
            |row| row.get(0),
        )?;
        let inheritance_changed = max_inheritance_edge_id > inheritance_watermark;
        // A Rust `use` edge written since the last pass, or a Rust item whose
        // visibility changed (`rust_import_epoch`, bumped by `Db::insert_edges`
        // and `Db::set_private_symbols`) can make a stored `crate::a::alias`
        // path resolvable (`Resolver::rust_follow_path`) without any symbol
        // named like the reference being new.
        let rust_import_epoch = self.get_meta_i64("rust_import_epoch")?.unwrap_or(0);
        let rust_imports_changed = rust_import_epoch
            > self
                .get_meta_i64("unresolved_reference_rust_import_epoch")?
                .unwrap_or(0);
        // A stored deferred-receiver row hangs on its callee's signature,
        // not on any symbol sharing its name, so it is always retried.
        let has_deferred_rows: bool = self.read_conn()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM unresolved_references
                 WHERE graph_version = ? AND edge_kind = 'CALLS'
                   AND deferred_kind IS NOT NULL)",
            params![graph_version],
            |row| row.get(0),
        )?;
        if !symbols_deleted_this_batch
            && !inheritance_changed
            && !rust_imports_changed
            && !has_deferred_rows
            && max_symbol_id <= watermark
        {
            return Ok(0);
        }

        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut total_resolved = 0;

        let candidates: Vec<StoreRetryRow> = {
            // Two independent ways a row earns a retry, unioned together
            // (each `SELECT DISTINCT` first, since either branch's join can
            // match a single `ur` row more than once):
            //
            // 1. The normal way -- some symbol past the watermark matches
            //    it by name, or (only when this batch deleted a symbol, and
            //    only for an `Ambiguous` row) any matching symbol at all:
            //    the surviving candidate that makes it unique again isn't
            //    new, so it'd never clear `s.id > ?2` on its own.
            // 2. Any row with a known receiver type, when this batch wrote
            //    a new inheritance-kind edge (see the method doc's finding
            //    G1) -- no symbol/name join at all, since the ancestor
            //    method a new EXTENDS/IMPLEMENTS/INHERITS edge newly makes
            //    reachable need not itself be new.
            //
            // Issue #79: the store is self-contained now, so neither branch
            // joins back to `edges` at all -- every column comes straight
            // off `ur`.
            let mut stmt = tx.prepare(
                "SELECT DISTINCT ur.id, ur.edge_id, ur.source_symbol_id, ur.file_id,
                        ur.edge_kind, ur.reference_name, ur.import_candidates,
                        ur.receiver_type, ur.bare_call, ur.detail, ur.evidence_snippet,
                        ur.evidence_start_line, ur.evidence_end_line, ur.confidence,
                        ur.commit_sha, ur.trace_id, ur.span_id, ur.event_ts,
                        COALESCE(f.language, 'unknown'), f.path, src.qualname, ur.call_shape,
                        ur.receiver_scope, ur.deferred_kind, ur.deferred
                 FROM unresolved_references ur
                 JOIN files f ON f.id = ur.file_id
                 LEFT JOIN symbols src ON src.id = ur.source_symbol_id
                 JOIN symbols s ON (s.qualname = ur.reference_name OR s.name = ur.name_tail)
                 WHERE ur.graph_version = ?1
                   AND s.graph_version = ?1
                   AND s.id > ?2

                 UNION

                 SELECT DISTINCT ur.id, ur.edge_id, ur.source_symbol_id, ur.file_id,
                        ur.edge_kind, ur.reference_name, ur.import_candidates,
                        ur.receiver_type, ur.bare_call, ur.detail, ur.evidence_snippet,
                        ur.evidence_start_line, ur.evidence_end_line, ur.confidence,
                        ur.commit_sha, ur.trace_id, ur.span_id, ur.event_ts,
                        COALESCE(f.language, 'unknown'), f.path, src.qualname, ur.call_shape,
                        ur.receiver_scope, ur.deferred_kind, ur.deferred
                 FROM unresolved_references ur
                 JOIN files f ON f.id = ur.file_id
                 LEFT JOIN symbols src ON src.id = ur.source_symbol_id
                 WHERE ur.graph_version = ?1
                   AND ((?4 AND ur.receiver_type IS NOT NULL AND ur.receiver_type != '')
                        OR ur.deferred_kind IS NOT NULL
                        OR (?3 AND ur.reason IN ('ambiguous', 'private'))
                        OR (?5 AND f.language = 'rust'
                            AND ur.edge_kind IN ('CALLS', 'USES', 'IMPORTS')
                            AND ur.reference_name LIKE '%::%')
                        OR (?5 AND f.language = 'rust' AND ur.reason = 'private'))",
            )?;
            let rows = stmt.query_map(
                params![
                    graph_version,
                    watermark,
                    symbols_deleted_this_batch,
                    inheritance_changed,
                    rust_imports_changed
                ],
                |row| {
                    Ok(StoreRetryRow {
                        store_id: row.get(0)?,
                        source_symbol_id: row.get(2)?,
                        file_id: row.get(3)?,
                        detail: row.get(9)?,
                        evidence_snippet: row.get(10)?,
                        evidence_start_line: row.get(11)?,
                        evidence_end_line: row.get(12)?,
                        confidence: row.get(13)?,
                        commit_sha: row.get(14)?,
                        trace_id: row.get(15)?,
                        span_id: row.get(16)?,
                        event_ts: row.get(17)?,
                        ctx: ReferenceContext {
                            edge_id: row.get(1)?,
                            edge_kind: row.get(4)?,
                            target_qualname: row.get(5)?,
                            import_candidates: row.get(6)?,
                            receiver_type: row.get(7)?,
                            receiver_scope: row.get(22)?,
                            deferred_kind: row.get(23)?,
                            deferred: row.get(24)?,
                            bare_call: row.get(8)?,
                            call_shape: row.get(21)?,
                            source_lang: row.get(18)?,
                            file_path: row.get(19)?,
                            source_qualname: row.get(20)?,
                            source_symbol_id: row.get(2)?,
                        },
                    })
                },
            )?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        {
            let mut resolver = Resolver::new(&tx, graph_version)?;
            let mut update_edge = tx.prepare(&UPDATE_EDGE_TARGET_SQL)?;
            // Issue #79: a pending, non-Bridge-Edge-kind row (`edge_id`
            // `None`) has no edge to update -- a successful retry inserts a
            // brand new one from this row's own shadow columns instead,
            // exactly as `Db::insert_edges` would have written it the first
            // time had it resolved then.
            let mut insert_edge = tx.prepare(
                "INSERT INTO edges
                 (file_id, source_symbol_id, target_symbol_id, kind, target_qualname, detail,
                  evidence_snippet, evidence_start_line, evidence_end_line, confidence,
                  graph_version, commit_sha, trace_id, span_id, event_ts, receiver_type,
                  resolution_kind, import_candidates, bare_call, call_shape, receiver_scope,
                  deferred_kind, deferred)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            let mut delete_store = tx.prepare("DELETE FROM unresolved_references WHERE id = ?")?;
            let mut relabel_store = tx.prepare(
                "UPDATE unresolved_references SET reason = ?1 WHERE id = ?2 AND reason != ?1",
            )?;
            let empty_symbol_map: HashMap<String, i64> = HashMap::new();

            for row in &candidates {
                let resolution = row.ctx.resolve(&mut resolver, &empty_symbol_map)?;
                // Still unresolved: relabel it with the reason this pass
                // classified, the one a fresh parse would record now.
                // A reference that now binds only a non-callable is no call:
                // a fresh index stores nothing for it.
                if resolution.unresolved_reason() == Some(UnresolvedReason::NotCallable) {
                    delete_store.execute(params![row.store_id])?;
                    continue;
                }
                if let Some(reason) = resolution.unresolved_reason() {
                    relabel_store.execute(params![reason.as_str(), row.store_id])?;
                }
                if let Resolution::Resolved { target_id, kind } = resolution {
                    match row.ctx.edge_id {
                        Some(edge_id) => {
                            update_edge.execute(params![
                                target_id,
                                kind.as_str(),
                                edge_id,
                                row.ctx.deferred_callee()
                            ])?;
                        }
                        None => {
                            insert_edge.execute(params![
                                row.file_id,
                                row.source_symbol_id,
                                target_id,
                                &row.ctx.edge_kind,
                                bound_target_qualname(
                                    &tx,
                                    row.ctx.deferred_kind.as_deref()
                                        == Some(DEFERRED_KIND_ARGUMENT),
                                    row.ctx.target_qualname.as_deref(),
                                    target_id,
                                )?,
                                row.detail.as_deref(),
                                row.evidence_snippet.as_deref(),
                                row.evidence_start_line,
                                row.evidence_end_line,
                                row.confidence,
                                graph_version,
                                row.commit_sha.as_deref(),
                                row.trace_id.as_deref(),
                                row.span_id.as_deref(),
                                row.event_ts,
                                row.ctx.receiver_type.as_deref(),
                                kind.as_str(),
                                row.ctx.import_candidates.as_deref(),
                                row.ctx.bare_call,
                                row.ctx.call_shape.as_deref(),
                                row.ctx.receiver_scope.as_deref(),
                                row.ctx.deferred_kind.as_deref(),
                                row.ctx.deferred.as_deref(),
                            ])?;
                        }
                    }
                    delete_store.execute(params![row.store_id])?;
                    total_resolved += 1;
                }
            }
        }

        // Advance both watermarks inside the same transaction, not via
        // `Db::set_meta_i64` after `commit` -- that would call `self.conn()`
        // again while `conn` (acquired above) is still holding the write
        // mutex, deadlocking against itself.
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('unresolved_reference_watermark', ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![max_symbol_id.to_string()],
        )?;
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('unresolved_reference_inheritance_watermark', ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![max_inheritance_edge_id.to_string()],
        )?;
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('unresolved_reference_rust_import_epoch', ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![rust_import_epoch.to_string()],
        )?;
        tx.commit()?;
        Ok(total_resolved)
    }

    /// Issue #80: an edge already bound to the external stub is still
    /// eligible for re-resolution -- a known-external outcome isn't always
    /// permanent. Unlike a genuinely external call (`requests.get`, never
    /// becoming a repo symbol), a reference the resolver couldn't attribute
    /// to a repo symbol only because that symbol didn't exist *yet* (a
    /// deleted file's module coming back is the incremental scenario this
    /// closes) must reattach once it does, the same as any other
    /// incrementally-repaired edge.
    ///
    /// Unlike `retry_unresolved_references`, this has no store row to key a
    /// targeted retry on (a stub-bound edge was never unresolved in the
    /// first place), so it re-judges every edge currently bound to a stub
    /// in `graph_version` -- same unconditional-rescan shape as
    /// `reconcile_unresolved_reference_store`'s own pass over NULL-target
    /// edges, bounded by how many known-external call sites exist, not by
    /// the whole edge table. Every column `ReferenceContext` needs to
    /// re-judge the reference is already on the edge row regardless of its
    /// resolution (`target_qualname`, `receiver_type`, `import_candidates`,
    /// `bare_call`), so no store round-trip is needed either way.
    ///
    /// Updates the edge in place only when it now resolves to a different
    /// target than before -- staying bound to the same stub (the common
    /// case: still genuinely external) is a no-op, not counted in the
    /// returned total.
    pub fn retry_external_stub_edges(&self, graph_version: i64) -> Result<usize> {
        self.rejudge_bound_edges(
            graph_version,
            "JOIN symbols stub ON stub.id = e.target_symbol_id",
            "AND stub.graph_version = ?1 AND stub.kind = 'external'",
        )
    }

    /// Re-judge every bound edge with a deferred receiver
    /// (`ReceiverType::Deferred`). Its target hangs on the *callee's*
    /// signature, so an edit to (or addition of) that callee -- which never
    /// touches this edge's own file or its bound target -- must retarget or
    /// unbind it, or incremental sync would diverge from a fresh reindex
    /// (issue #77). Same rescan shape as `retry_external_stub_edges`,
    /// bounded by the deferred call sites.
    pub fn retry_deferred_receiver_edges(&self, graph_version: i64) -> Result<usize> {
        self.rejudge_bound_edges(
            graph_version,
            "",
            "AND e.target_symbol_id IS NOT NULL AND e.kind = 'CALLS'
             AND e.deferred_kind IS NOT NULL",
        )
    }

    /// Re-judge every edge bound through a Rust re-export
    /// (`ResolutionKind::Reexport`). Its target hangs on a `use` declaration
    /// in a module the edge's own file never touches, so editing or deleting
    /// that declaration must retarget or unbind the edge, or incremental sync
    /// would diverge from a fresh reindex (issue #77).
    pub fn retry_rust_reexport_edges(&self, graph_version: i64) -> Result<usize> {
        self.rejudge_bound_edges(
            graph_version,
            "",
            "AND e.target_symbol_id IS NOT NULL AND e.resolution_kind = 'reexport'",
        )
    }

    /// Rebuild the `RPC_CALL` edges of calls through a deferred receiver
    /// (`ReceiverType::Deferred`) whose callee returns a generated gRPC
    /// client (`var c = CreateClient(); c.SayHello()` with the factory in
    /// another file). The extractor can't see that return type, so the call
    /// site's `CALLS` edge (or its unresolved-store row) is re-read here and
    /// the edges derived from the callee's declaration. Last pass's derived
    /// edges (`derived = 1`) go first, so the outcome
    /// depends only on the current symbols and incremental sync equals a
    /// fresh reindex (issue #77). Returns how many edges it wrote.
    pub fn rederive_deferred_rpc_calls(&self, graph_version: i64) -> Result<usize> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM edges WHERE graph_version = ?1 AND derived = 1",
            params![graph_version],
        )?;
        let sites = load_deferred_call_sites(&tx, graph_version)?;
        let derived = {
            let mut resolver = Resolver::new(&tx, graph_version)?;
            derive_rpc_calls(&mut resolver, &sites)?
        };
        let written = insert_derived_rpc_calls(&tx, graph_version, &derived)?;
        tx.commit()?;
        Ok(written)
    }

    /// Re-run `Resolver::resolve` on the edges of `graph_version` selected
    /// by `extra_join`/`extra_where` (both spliced into the query), updating
    /// one that now resolves elsewhere and unbinding one that no longer
    /// resolves. Returns how many changed.
    fn rejudge_bound_edges(
        &self,
        graph_version: i64,
        extra_join: &str,
        extra_where: &str,
    ) -> Result<usize> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut total_resolved = 0;

        struct StubEdgeRow {
            edge_id: i64,
            target_symbol_id: i64,
            ctx: ReferenceContext,
        }

        let rows: Vec<StubEdgeRow> = {
            let mut stmt = tx.prepare(&format!(
                "SELECT e.id, e.target_symbol_id, e.kind, e.target_qualname,
                        e.receiver_type, e.import_candidates, e.bare_call,
                        COALESCE(f.language, 'unknown'), f.path, src.qualname, e.call_shape,
                        e.receiver_scope, e.deferred_kind, e.deferred, e.source_symbol_id
                 FROM edges e
                 {extra_join}
                 JOIN files f ON f.id = e.file_id
                 LEFT JOIN symbols src ON src.id = e.source_symbol_id
                 WHERE e.graph_version = ?1 {extra_where}"
            ))?;
            let out = stmt.query_map(params![graph_version], |row| {
                Ok(StubEdgeRow {
                    edge_id: row.get(0)?,
                    target_symbol_id: row.get(1)?,
                    ctx: ReferenceContext {
                        edge_id: Some(row.get(0)?),
                        edge_kind: row.get(2)?,
                        target_qualname: row.get(3)?,
                        receiver_type: row.get(4)?,
                        receiver_scope: row.get(11)?,
                        deferred_kind: row.get(12)?,
                        deferred: row.get(13)?,
                        import_candidates: row.get(5)?,
                        bare_call: row.get(6)?,
                        call_shape: row.get(10)?,
                        source_lang: row.get(7)?,
                        file_path: row.get(8)?,
                        source_qualname: row.get(9)?,
                        source_symbol_id: row.get(14)?,
                    },
                })
            })?;
            out.collect::<rusqlite::Result<Vec<_>>>()?
        };

        {
            let mut resolver = Resolver::new(&tx, graph_version)?;
            let mut update_edge = tx.prepare(&UPDATE_EDGE_TARGET_SQL)?;
            let empty_symbol_map: HashMap<String, i64> = HashMap::new();

            for row in &rows {
                match row.ctx.resolve(&mut resolver, &empty_symbol_map)? {
                    Resolution::Resolved { target_id, kind }
                        if target_id != row.target_symbol_id =>
                    {
                        update_edge.execute(params![
                            target_id,
                            kind.as_str(),
                            row.edge_id,
                            row.ctx.deferred_callee()
                        ])?;
                        total_resolved += 1;
                    }
                    // A fresh index would leave this reference unresolved,
                    // so unbind it from the stub; the caller's follow-up
                    // `reconcile_unresolved_reference_store` moves it into
                    // the store with its reason.
                    Resolution::Unresolved(_) => {
                        update_edge.execute(params![
                            None::<i64>,
                            None::<String>,
                            row.edge_id,
                            row.ctx.deferred_callee()
                        ])?;
                        total_resolved += 1;
                    }
                    _ => {}
                }
                // A tier-3 stub (stored `receiver_type == Some("")`, see
                // `Resolution::stored_receiver_type`) can't come back
                // `Unresolved` here: every tier that can refuse (ambiguous,
                // private, no candidates) requires `receiver_type` to be
                // `None`/a known type first, so rerunning `resolve` on the
                // same `Some("")` input takes the same tier again, which
                // always resolves (to the same stub -- genuinely external
                // stays that way).
                //
                // A tier-6 stub (`via_language_fallback`, stored
                // `receiver_type` left as originally extracted -- `None` for
                // Rust/Go) *can* come back `Unresolved` here: its stored
                // `receiver_type` no longer short-circuits straight to the
                // stub, so this rerun actually exercises the name tiers
                // again (the whole point -- see `ResolutionKind::External`'s
                // doc). If that now finds more than one candidate or only a
                // private one, the edge is unbound above so it ends up
                // unresolved exactly as a fresh index would leave it.
            }
        }

        tx.commit()?;
        Ok(total_resolved)
    }

    /// Unresolved reference counts by reason, grouped by language, for the
    /// golden-corpus scoreboard (issue #78) -- how many `unresolved_references`
    /// rows `graph_version` currently holds, per `(files.language, reason)`
    /// pair. Test/reporting support, same spirit as `edges_snapshot`.
    pub fn unresolved_reference_summary(
        &self,
        graph_version: i64,
    ) -> Result<Vec<crate::model::UnresolvedReferenceSummary>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT COALESCE(f.language, 'unknown'), ur.reason, COUNT(*)
             FROM unresolved_references ur
             JOIN files f ON f.id = ur.file_id
             WHERE ur.graph_version = ?
             GROUP BY f.language, ur.reason
             ORDER BY f.language, ur.reason",
        )?;
        let rows = stmt.query_map(params![graph_version], |row| {
            Ok(crate::model::UnresolvedReferenceSummary {
                language: row.get(0)?,
                reason: row.get(1)?,
                count: row.get(2)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// Count of `unresolved_references` rows at `graph_version` whose
    /// `source_symbol_id` is one of `symbol_ids` -- the minimum lower-bound
    /// signal `trace_flow`/`analyze_impact` attach to a traversal (issue
    /// #81): a traversed symbol had at least one outgoing reference the
    /// write path couldn't attribute a target to, so the traversed graph
    /// may be missing edges from it.
    ///
    /// Deliberately the same query for both traversal directions. For a
    /// downstream trace this is exact: a pending row's source symbol is
    /// exactly a symbol the BFS visited, so a missing outgoing edge from it
    /// is a real gap in what was traversed. For an upstream trace it's a
    /// conservative minimum, not a full account of missing callers: an
    /// unresolved reference whose name_tail happens to match a traversed
    /// symbol, but whose own source symbol was never visited (because the
    /// reference never resolved to it -- exactly the caller
    /// `analyze_impact`/`trace_flow` failed to find), can't be counted this
    /// way without fuzzy name-tail matching, which issue #81 explicitly
    /// keeps out of scope. What upstream does get: any traversed caller
    /// that itself has further unresolved outgoing references still flags
    /// the answer as a lower bound.
    pub fn unresolved_reference_count_for_symbols(
        &self,
        symbol_ids: &[i64],
        graph_version: i64,
    ) -> Result<i64> {
        if symbol_ids.is_empty() {
            return Ok(0);
        }
        let mut placeholders = String::new();
        for (idx, _) in symbol_ids.iter().enumerate() {
            if idx > 0 {
                placeholders.push(',');
            }
            placeholders.push('?');
        }
        let sql = format!(
            "SELECT COUNT(*) FROM unresolved_references \
             WHERE source_symbol_id IN ({placeholders}) AND graph_version = ?"
        );
        let conn = self.read_conn()?;
        let mut params: Vec<&dyn rusqlite::ToSql> = symbol_ids
            .iter()
            .map(|id| id as &dyn rusqlite::ToSql)
            .collect();
        params.push(&graph_version);
        conn.query_row(&sql, &*params, |row| row.get(0))
            .map_err(Into::into)
    }

    /// The repair pass shared by `Indexer::sync_abs_paths`'s post-batch hook
    /// and `Indexer::reindex`'s `needs_repair` branch (issue #78/#79): both
    /// used to duplicate the same reconcile-then-retry-then-`eprintln!`
    /// sequence inline. Reconciles first (see
    /// `reconcile_unresolved_reference_store`'s doc for why), then runs the
    /// targeted, store-driven retry (`retry_unresolved_references`),
    /// printing a one-line summary for whichever step did anything.
    /// `context` names the caller for that summary (e.g. "incremental sync",
    /// "reindex"). Returns `(reconciled, store_resolved)`.
    pub fn repair_unresolved(
        &self,
        graph_version: i64,
        symbols_deleted_this_batch: bool,
        context: &str,
    ) -> Result<(usize, usize)> {
        let reconciled = self.reconcile_unresolved_reference_store(graph_version)?;
        if reconciled > 0 {
            eprintln!("lidx: reconciled {reconciled} unresolved reference(s) after {context}");
        }
        // Before the store retry: an edge unbound here is moved into the
        // store by a reconcile and retried with the rest.
        let deferred_rejudged = self.retry_deferred_receiver_edges(graph_version)?;
        if deferred_rejudged > 0 {
            eprintln!(
                "lidx: re-judged {deferred_rejudged} deferred-receiver edge(s) after {context}"
            );
            self.reconcile_unresolved_reference_store(graph_version)?;
        }
        let reexports_rejudged = self.retry_rust_reexport_edges(graph_version)?;
        if reexports_rejudged > 0 {
            eprintln!(
                "lidx: re-judged {reexports_rejudged} Rust re-export edge(s) after {context}"
            );
            self.reconcile_unresolved_reference_store(graph_version)?;
        }
        let store_resolved =
            self.retry_unresolved_references(graph_version, symbols_deleted_this_batch)?;
        if store_resolved > 0 {
            eprintln!(
                "lidx: resolved {store_resolved} stored unresolved reference(s) after {context}"
            );
        }
        let stub_edges_resolved = self.retry_external_stub_edges(graph_version)?;
        if stub_edges_resolved > 0 {
            eprintln!(
                "lidx: reattached {stub_edges_resolved} edge(s) off the external stub after {context}"
            );
            // Edges unbound from a stub are now NULL-target; move them into
            // the store like any other orphaned edge.
            self.reconcile_unresolved_reference_store(graph_version)?;
        }
        // After every step that can bind a deferred call site, since it
        // reads both bound edges and store rows.
        let deferred_rpc = self.rederive_deferred_rpc_calls(graph_version)?;
        if deferred_rpc > 0 {
            self.reconcile_unresolved_reference_store(graph_version)?;
        }
        // Runs after `retry_external_stub_edges`, so a stub an edge just
        // moved off of in this same pass is pruned immediately if that was
        // its last caller, not left one repair pass behind.
        let pruned_stubs = self.prune_orphan_external_symbols(graph_version)?;
        if pruned_stubs > 0 {
            eprintln!(
                "lidx: pruned {pruned_stubs} orphaned external stub symbol(s) after {context}"
            );
        }
        Ok((reconciled, store_resolved))
    }

    /// Issue #80: delete every external stub symbol (`kind = 'external'`)
    /// in `graph_version` with no remaining incoming edge. A stub is
    /// created lazily, on demand, the first time some reference resolves
    /// to it (`Resolver::resolve_external_stub`) -- a fresh reindex would
    /// never create one that nothing calls, so an existing stub whose last
    /// caller just disappeared (an edited-away call, or that caller's
    /// whole file deleted) must not linger either, or incremental sync
    /// would permanently diverge from what a fresh reindex produces.
    ///
    /// Called from `repair_unresolved`, after the rest of the repair pass
    /// -- both `Indexer::sync_abs_paths` (every touched batch) and
    /// `Indexer::reindex` (whenever `needs_repair`, which already covers
    /// every reindex that indexed or deleted a file) reach this. On a
    /// warm, nothing-changed reindex neither runs, but nothing needs
    /// pruning then either: yesterday's stubs already reflect yesterday's
    /// callers.
    ///
    /// `Db::carry_forward_references` unconditionally copies every stub forward
    /// into a new graph_version regardless of whether its callers were
    /// among the carried-forward files (simplest way to guarantee a stub a
    /// carried edge still targets exists there for its stable_id-based
    /// remap to find) -- this is what cleans up the ones that copy
    /// brought along but nothing actually calls anymore.
    pub(crate) fn prune_orphan_external_symbols(&self, graph_version: i64) -> Result<usize> {
        let conn = self.conn();
        Ok(conn.execute(
            "DELETE FROM symbols
             WHERE graph_version = ?1 AND kind = 'external'
               AND NOT EXISTS (
                 SELECT 1 FROM edges e
                 WHERE e.target_symbol_id = symbols.id AND e.graph_version = ?1
               )",
            params![graph_version],
        )?)
    }
}

/// One `retry_unresolved_references` candidate row: `store_id` (to delete
/// the store row on a successful retry), `source_symbol_id`/`file_id` plus
/// the shadow columns (for a fresh edge insert on success -- issue #79,
/// since a pending non-Bridge-Edge-kind row has no edge to update instead),
/// and everything `Resolver::resolve` needs to retry it -- all read
/// straight from `unresolved_references` itself now (self-contained, no
/// join to `edges`). See `ReferenceContext`'s doc for why the resolve-only
/// fields are shared with `NullTargetEdgeRow` rather than duplicated.
struct StoreRetryRow {
    store_id: i64,
    source_symbol_id: Option<i64>,
    file_id: i64,
    detail: Option<String>,
    evidence_snippet: Option<String>,
    evidence_start_line: Option<i64>,
    evidence_end_line: Option<i64>,
    confidence: Option<f64>,
    commit_sha: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    event_ts: Option<i64>,
    ctx: ReferenceContext,
}

/// One `reconcile_unresolved_reference_store` candidate row:
/// `source_symbol_id`/`file_id` plus the shadow columns (for a fresh store
/// insert if still unresolved -- issue #79) and everything
/// `Resolver::resolve` needs to re-judge a NULL-target edge that has no
/// `unresolved_references` row yet, read straight from `edges` and its
/// file/source-symbol joins -- there's no store row to join against here,
/// unlike `StoreRetryRow`. See `ReferenceContext`'s doc for why the
/// resolve-only fields are shared rather than duplicated.
struct NullTargetEdgeRow {
    source_symbol_id: Option<i64>,
    file_id: i64,
    detail: Option<String>,
    evidence_snippet: Option<String>,
    evidence_start_line: Option<i64>,
    evidence_end_line: Option<i64>,
    confidence: Option<f64>,
    commit_sha: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    event_ts: Option<i64>,
    ctx: ReferenceContext,
}

/// A call through a deferred receiver, as stored on its `CALLS` edge or
/// unresolved-store row.
struct DeferredCallSite {
    file_id: i64,
    source_symbol_id: Option<i64>,
    /// The deferred receiver, and its stored payload (the cache key).
    marker: Option<DeferredMarker>,
    payload: String,
    /// The call's target text (`c.SayHello`).
    target: String,
    snippet: Option<String>,
    start_line: Option<i64>,
    end_line: Option<i64>,
    confidence: Option<f64>,
    commit_sha: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    event_ts: Option<i64>,
    lang: String,
    path: String,
}

/// (marker, method, language): what a deferred RPC derivation depends on.
type RpcCacheKey<'a> = (&'a str, &'a str, &'a str);

/// An `RPC_CALL` edge derived for a `DeferredCallSite`, resolved.
struct DerivedRpcCall<'a> {
    site: &'a DeferredCallSite,
    edge: RpcCallEdge,
    target_id: Option<i64>,
    resolution_kind: Option<&'static str>,
}

/// Every deferred `CALLS` site of `graph_version`, bound (`edges`) or not
/// (`unresolved_references`). Both scans hit partial indexes on
/// `deferred_kind` (migration 24).
fn load_deferred_call_sites(
    conn: &Connection,
    graph_version: i64,
) -> Result<Vec<DeferredCallSite>> {
    let mut sites = Vec::new();
    for (table, kind_col, target_col) in [
        ("edges", "kind", "target_qualname"),
        ("unresolved_references", "edge_kind", "reference_name"),
    ] {
        let mut stmt = conn.prepare(&format!(
            "SELECT t.file_id, t.source_symbol_id, t.deferred, t.{target_col},
                    t.evidence_snippet, t.evidence_start_line, t.evidence_end_line,
                    t.confidence, t.commit_sha, t.trace_id, t.span_id, t.event_ts,
                    COALESCE(f.language, 'unknown'), f.path
             FROM {table} t JOIN files f ON f.id = t.file_id
             WHERE t.graph_version = ?1 AND t.{kind_col} = 'CALLS'
               AND t.deferred_kind = '{DEFERRED_KIND_RETURN}'
               AND t.{target_col} IS NOT NULL
             ORDER BY t.id"
        ))?;
        let rows = stmt.query_map(params![graph_version], |row| {
            let payload: String = row.get(2)?;
            Ok(DeferredCallSite {
                file_id: row.get(0)?,
                source_symbol_id: row.get(1)?,
                marker: DeferredMarker::decode(DEFERRED_KIND_RETURN, &payload),
                payload,
                target: row.get(3)?,
                snippet: row.get(4)?,
                start_line: row.get(5)?,
                end_line: row.get(6)?,
                confidence: row.get(7)?,
                commit_sha: row.get(8)?,
                trace_id: row.get(9)?,
                span_id: row.get(10)?,
                event_ts: row.get(11)?,
                lang: row.get(12)?,
                path: row.get(13)?,
            })
        })?;
        for row in rows {
            let site = row?;
            if site.marker.is_some() {
                sites.push(site);
            }
        }
    }
    Ok(sites)
}

/// The `RPC_CALL` edges of each site whose callee returns a client, with
/// their targets resolved. Sites sharing a marker and method are judged once.
fn derive_rpc_calls<'a>(
    resolver: &mut Resolver<'_>,
    sites: &'a [DeferredCallSite],
) -> Result<Vec<DerivedRpcCall<'a>>> {
    let no_symbols: HashMap<String, i64> = HashMap::new();
    let mut cache: HashMap<RpcCacheKey<'_>, Vec<(String, String)>> = HashMap::new();
    let mut derived = Vec::new();
    for site in sites {
        let (Some(hook), Some(marker)) = (profile_for(&site.lang).deferred_rpc, &site.marker)
        else {
            continue;
        };
        let method = qualname_trailing_name(&site.target);
        let key = (site.payload.as_str(), method, site.lang.as_str());
        if let std::collections::hash_map::Entry::Vacant(slot) = cache.entry(key) {
            let index = LanguageIndex {
                resolver,
                lang: &site.lang,
            };
            let edges = hook(marker, method, &index)?.unwrap_or_default();
            let edges = edges
                .into_iter()
                .map(|e| (e.target_qualname, e.detail))
                .collect::<Vec<_>>();
            slot.insert(edges);
        }
        for (target_qualname, detail) in &cache[&key] {
            let resolution = resolver.resolve(
                &Reference {
                    target_qualname: Some(target_qualname),
                    edge_kind: "RPC_CALL",
                    receiver_type: None,
                    receiver_scope: None,
                    deferred: None,
                    import_candidates: &[],
                    source_lang: &site.lang,
                    source_file_path: &site.path,
                    source_qualname: None,
                    source_symbol_id: None,
                    bare_call: false,
                    call_shape: None,
                },
                &no_symbols,
            )?;
            derived.push(DerivedRpcCall {
                site,
                edge: RpcCallEdge {
                    target_qualname: target_qualname.clone(),
                    detail: detail.clone(),
                },
                target_id: resolution.target_id(),
                resolution_kind: resolution.kind_column(),
            });
        }
    }
    Ok(derived)
}

fn insert_derived_rpc_calls(
    conn: &Connection,
    graph_version: i64,
    derived: &[DerivedRpcCall<'_>],
) -> Result<usize> {
    let mut insert = conn.prepare(
        "INSERT INTO edges
         (file_id, source_symbol_id, target_symbol_id, kind, target_qualname, detail,
          evidence_snippet, evidence_start_line, evidence_end_line, confidence,
          graph_version, commit_sha, trace_id, span_id, event_ts, resolution_kind, derived)
         VALUES (?, ?, ?, 'RPC_CALL', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
    )?;
    for d in derived {
        let site = d.site;
        insert.execute(params![
            site.file_id,
            site.source_symbol_id,
            d.target_id,
            d.edge.target_qualname,
            d.edge.detail,
            site.snippet,
            site.start_line,
            site.end_line,
            site.confidence,
            graph_version,
            site.commit_sha,
            site.trace_id,
            site.span_id,
            site.event_ts,
            d.resolution_kind,
        ])?;
    }
    Ok(derived.len())
}

/// Encode `EdgeInput::import_candidates` for the `edges.import_candidates`
/// column: `None` (SQL NULL) when there are none — the common case — so
/// the repair pass can select rows worth retrying with `IS NOT NULL`.
pub(crate) fn encode_import_candidates(candidates: &[String]) -> Option<String> {
    if candidates.is_empty() {
        None
    } else {
        serde_json::to_string(candidates).ok()
    }
}

/// Inverse of `encode_import_candidates`. Malformed JSON decodes to an
/// empty list rather than failing the whole repair pass.
fn decode_import_candidates(raw: &str) -> Vec<String> {
    serde_json::from_str(raw).unwrap_or_default()
}

/// The `unresolved_references.reference_name`/`name_tail` pair to store for
/// an `Unresolved` reference (issue #78): the edge's own `target_qualname`
/// text when it has one, else its first import candidate (an
/// `IMPORTS_FILE` edge has no literal call-site text, only import-qualified
/// candidates). `None` when neither exists — nothing for a retry to key
/// on — so `Db::insert_edges` writes no store row for it at all.
pub(crate) fn store_reference_name_and_tail(
    target_qualname: Option<&str>,
    import_candidates: &[String],
) -> Option<(Option<String>, String)> {
    if let Some(qn) = target_qualname {
        return Some((Some(qn.to_string()), qualname_trailing_name(qn).to_string()));
    }
    let first = import_candidates.first()?;
    Some((None, qualname_trailing_name(first).to_string()))
}

/// The language value the same-language tiers compare against.
/// `typescript`, `tsx` and `javascript` are separate `files.language`
/// values only because each needs its own tree-sitter grammar; for
/// resolution they are one family. Must agree with the `CASE` in
/// `SAME_LANG_SQL`.
///
/// Safe from the `FakeClock.now` class of bug only because the JS/TS
/// extractor records an import candidate for every call through an import
/// binding, so a missed import refuses the name-based tiers outright.
fn resolution_language_family(lang: &str) -> &str {
    match lang {
        "typescript" | "tsx" => "javascript",
        other => other,
    }
}

/// Maximum EXTENDS/IMPLEMENTS/INHERITS hops `resolve_via_inheritance`
/// follows. Exists so a cyclic or pathological hierarchy can't turn one
/// call into unbounded work.
///
/// ponytail: a flat hop cap, not cycle detection — `seen` still dedupes
/// visited symbols, but the cap bounds worst-case cost. 8 is comfortably
/// past any hierarchy depth seen in real corpora.
const MAX_INHERITANCE_DEPTH: usize = 8;

/// Extract the trailing name segment from a qualname, handling both `.` and `::` separators.
///
/// Examples:
/// - `"a.b.process"` → `"process"`
/// - `"crate::util::helper::process"` → `"process"`
/// - `"_svc.DeployAsync"` → `"DeployAsync"`
/// - `"process"` → `"process"` (no separator)
pub(crate) fn qualname_trailing_name(qn: &str) -> &str {
    match last_qualname_separator(qn) {
        Some(start) => &qn[start..],
        None => qn,
    }
}

/// Build the bare-name suffix-match inputs for a target qualname: the
/// trailing name plus LIKE patterns for both `.`- and `::`-separated
/// qualnames. Deliberately no bare `%name` pattern: that would let
/// `process` match `reprocess`.
fn fuzzy_qualname_patterns(qn: &str) -> (&str, String, String) {
    let name = qualname_trailing_name(qn);
    (name, format!("%.{name}"), format!("%::{name}"))
}

/// A language's primary (first-declared) qualname separator, for joining a
/// type name and a method name before a two-segment lookup — e.g. Rust's
/// `Greeter` + `greet` becomes `Greeter::greet`, not `Greeter.greet`.
fn primary_separator(source_lang: &str) -> &'static str {
    profile_for(source_lang)
        .separators
        .first()
        .copied()
        .unwrap_or(".")
}

/// Same-language LIKE patterns for a trailing name, using only the
/// separator(s) `profile` declares — e.g. a Rust same-language round never
/// bothers checking a `.`-suffix pattern, since no Rust qualname contains
/// one. Every profile today declares exactly one separator, so both SQL
/// slots get that same pattern; a future two-separator profile would get
/// one each.
fn same_lang_patterns(name: &str, profile: &LanguageProfile) -> (String, String) {
    let first = profile.separators.first().copied().unwrap_or(".");
    let second = profile.separators.get(1).copied().unwrap_or(first);
    (format!("%{first}{name}"), format!("%{second}{name}"))
}

/// Case-sensitive re-check for a `SAME_LANG_SQL`/`ANY_LANG_SQL` candidate
/// row (issue #110): SQLite's `LIKE` is case-insensitive for ASCII, so
/// those queries' `WHERE (qualname = ? OR qualname LIKE ? OR qualname LIKE
/// ?)` clause alone would let e.g. `Error` match a same-named lowercase
/// `error`. Every `LIKE` pattern this module ever builds
/// (`same_lang_patterns`, `fuzzy_qualname_patterns`, `unique_by_pattern`'s
/// `any_lang_patterns`) is one of exactly two shapes — an exact-qualname
/// match, or a qualname ending in `.name`/`::name` (the only two
/// separators `last_qualname_separator` ever recognizes) — so re-deriving
/// that same match here, where `==`/`ends_with` are always byte-exact
/// regardless of what characters `name` contains, needs no wildcard
/// escaping the way a `GLOB` rewrite of the same pattern would.
fn matches_name_case_sensitive(qualname: &str, name: &str) -> bool {
    qualname == name
        || qualname
            .strip_suffix(name)
            .is_some_and(|prefix| prefix.ends_with('.') || prefix.ends_with("::"))
}

/// Whether `NAME_PREFILTER` is result-preserving for a lookup whose trailing
/// segment is `tail`: not for TS/JS computed or quoted method keys
/// (`[Symbol.iterator]`, `'a.b'`), whose `name` is longer than the qualname's
/// last segment and whose tail ends in `]` or a quote.
fn name_prefilter_applies(tail: &str) -> bool {
    !tail.is_empty() && !tail.ends_with([']', '\'', '"', '`'])
}

/// Index right after the last qualname separator (`.` or `::`) in `s`, or
/// `None` when `s` has none.
fn last_qualname_separator(s: &str) -> Option<usize> {
    let dot_pos = s.rfind('.').map(|p| p + 1);
    let colons_pos = s.rfind("::").map(|p| p + 2);
    match (dot_pos, colons_pos) {
        (Some(d), Some(c)) => Some(d.max(c)),
        (Some(d), None) => Some(d),
        (None, Some(c)) => Some(c),
        (None, None) => None,
    }
}

/// The trailing **two** qualname segments, when there are more than one.
///
/// - `"crate::db::Db::new"` -> `Some("Db::new")`
/// - `"pkg.store.EventStore.append"` -> `Some("EventStore.append")`
/// - `"process"` -> `None`
fn qualname_trailing_two_segments(qn: &str) -> Option<&str> {
    let name_start = last_qualname_separator(qn)?;
    let prefix = qn[..name_start].trim_end_matches(['.', ':']);
    if prefix.is_empty() {
        return None;
    }
    let seg2_start = last_qualname_separator(prefix).unwrap_or(0);
    Some(&qn[seg2_start..])
}

/// Suffix-match inputs for the last two segments (`Type::method` /
/// `Type.method`); `None` when the qualname carries only one segment.
///
/// ponytail: matching the last two segments already present in the target
/// string is the cheap fix for the `Type::method` call shape (`Db::new`
/// finds `crate::db::Db::new` without colliding with `Vec::new`). It is not
/// type inference: it won't help one-segment qualnames or two unrelated
/// types sharing both segments. Resolving the receiver's actual type is the
/// upgrade path.
fn two_segment_qualname_patterns(qn: &str) -> Option<(String, String, String)> {
    let two = qualname_trailing_two_segments(qn)?;
    Some((two.to_string(), format!("%.{two}"), format!("%::{two}")))
}

#[cfg(test)]
mod tests {
    use super::{
        Arity, ImportMissPolicy, LanguageProfile, Reference, Resolution, ResolutionKind, Resolver,
        UnresolvedReason, VisibilityRule, arity_admits, fuzzy_qualname_patterns, package_dir,
        primary_separator, profile_for, qualname_trailing_name, same_lang_patterns,
        two_segment_qualname_patterns,
    };
    use rusqlite::{Connection, params};

    fn arity(args: usize, value_receiver: bool) -> Option<Arity> {
        Some(Arity {
            args,
            value_receiver,
        })
    }

    #[test]
    fn arity_admits_reads_parameter_counts_from_a_csharp_signature() {
        let admits = |sig: &str, a| arity_admits(a, "method", Some(sig));
        assert!(admits("(int a, int b) -> int", arity(2, false)));
        assert!(!admits("(int a, int b) -> int", arity(1, false)));
        assert!(admits("(Dictionary<string, int> a)", arity(1, false)));
        assert!(admits("(int a, int b = 0)", arity(1, false)));
        assert!(admits("(params int[] xs)", arity(5, false)));
        assert!(admits("()", arity(0, false)));
        // No arity signal, or nothing to compare: never excludes.
        assert!(arity_admits(None, "method", Some("(int a)")));
        assert!(arity_admits(arity(3, false), "class", None));
    }

    #[test]
    fn arity_admits_counts_the_extension_receiver_only_for_a_value_receiver() {
        let one = "(this Kind kind) -> string";
        let two = "(this Kind kind, int pad) -> string";
        assert!(arity_admits(arity(0, true), "method", Some(one)));
        assert!(!arity_admits(arity(1, true), "method", Some(one)));
        assert!(arity_admits(arity(1, true), "method", Some(two)));
        // `KindExt.ToDb(kind)` -- a type receiver passes the receiver explicitly.
        assert!(arity_admits(arity(1, false), "method", Some(one)));
    }

    #[test]
    fn profile_for_registers_every_visibility_rule() {
        assert_eq!(profile_for("rust").separators, &["::"]);
        assert_eq!(
            profile_for("rust").import_miss,
            ImportMissPolicy::FallThrough
        );
        assert!(profile_for("rust").normalize_import_target.is_some());
        assert_eq!(profile_for("rust").visibility, VisibilityRule::RustModule);

        assert_eq!(profile_for("python").separators, &["."]);
        assert_eq!(
            profile_for("python").import_miss,
            ImportMissPolicy::PythonRepoHeuristic
        );
        assert!(profile_for("python").normalize_import_target.is_none());
        // Issue #75 gives Rust/C#/TypeScript/Go a visibility rule, not
        // Python — no enforced private/public distinction worth tracking
        // for it (see `VisibilityRule::None`'s doc).
        assert_eq!(profile_for("python").visibility, VisibilityRule::None);

        // C#, JavaScript/TypeScript and Go each register a profile now too
        // (issue #75) -- separators/import_miss/normalize_import_target
        // stay the shared default, only `visibility` differs.
        for lang in ["csharp", "javascript", "typescript", "tsx"] {
            assert_eq!(profile_for(lang).separators, &["."], "{lang}");
            assert_eq!(
                profile_for(lang).import_miss,
                ImportMissPolicy::Refuse,
                "{lang}"
            );
            assert!(
                profile_for(lang).normalize_import_target.is_none(),
                "{lang}"
            );
            assert_eq!(
                profile_for(lang).visibility,
                VisibilityRule::Recorded,
                "{lang}"
            );
        }
        assert_eq!(
            profile_for("go").visibility,
            VisibilityRule::GoCapitalization
        );

        // A language absent from any indexed repo still gets the shared,
        // fully-permissive default.
        assert_eq!(profile_for("made-up-lang").visibility, VisibilityRule::None);
        assert_eq!(
            profile_for("made-up-lang").import_miss,
            ImportMissPolicy::Refuse
        );
    }

    #[test]
    fn primary_separator_reads_the_profile() {
        assert_eq!(primary_separator("rust"), "::");
        assert_eq!(primary_separator("python"), ".");
        assert_eq!(primary_separator("csharp"), ".");
    }

    #[test]
    fn same_lang_patterns_use_only_the_profiles_separators() {
        assert_eq!(
            same_lang_patterns("greet", &LanguageProfile::DEFAULT),
            ("%.greet".to_string(), "%.greet".to_string())
        );
        assert_eq!(
            same_lang_patterns("greet", &profile_for("rust")),
            ("%::greet".to_string(), "%::greet".to_string())
        );
    }

    #[test]
    fn fuzzy_patterns_anchor_on_a_separator() {
        // No bare `%name`: `process` must never match `reprocess`.
        assert_eq!(
            fuzzy_qualname_patterns("helper::process"),
            ("process", "%.process".to_string(), "%::process".to_string())
        );
        // Degenerate targets yield an empty name, which no real qualname matches.
        for degenerate in ["util::", "util.", ""] {
            assert_eq!(fuzzy_qualname_patterns(degenerate).0, "", "{degenerate:?}");
        }
        // A lone ':' is part of the name, not a separator.
        assert_eq!(fuzzy_qualname_patterns("Foo:process").0, "Foo:process");
    }

    #[test]
    fn two_segment_patterns_need_two_segments() {
        assert_eq!(
            two_segment_qualname_patterns("crate::db::Db::new"),
            Some((
                "Db::new".to_string(),
                "%.Db::new".to_string(),
                "%::Db::new".to_string()
            ))
        );
        assert_eq!(
            two_segment_qualname_patterns("pkg.store.EventStore.append").map(|p| p.0),
            Some("EventStore.append".to_string())
        );
        assert_eq!(two_segment_qualname_patterns("process"), None);
        assert_eq!(two_segment_qualname_patterns("::process"), None);
    }

    #[test]
    fn test_qualname_trailing_name() {
        let cases: &[(&str, &str)] = &[
            // '.' separator
            ("a.b.process", "process"),
            ("_svc.DeployAsync", "DeployAsync"),
            // '::' separator
            ("crate::util::helper::process", "process"),
            ("foo::bar", "bar"),
            // no separator
            ("process", "process"),
            // mixed separators: last one wins
            ("crate::Foo.method", "method"),
            ("pkg.module::func", "func"),
            // trailing separators yield an empty name; downstream patterns
            // ('', '%.', '%::') cannot match any real qualname
            ("foo.", ""),
            ("foo::", ""),
            // leading separators are stripped
            (".foo", "foo"),
            ("::foo", "foo"),
            // a lone ':' (not '::') is part of the name, never a split point
            ("label:name", "label:name"),
            ("a::b:c", "b:c"),
            (":", ":"),
            // degenerate inputs
            ("", ""),
            (".", ""),
            ("::", ""),
            // repeated separators collapse to the last one
            ("a..b", "b"),
            ("a:::b", "b"),
        ];
        for (input, expected) in cases {
            assert_eq!(qualname_trailing_name(input), *expected, "input: {input:?}");
        }
    }

    #[test]
    fn package_dir_reads_the_prefix_before_the_last_slash() {
        assert_eq!(package_dir("caller/caller.Entry"), "caller");
        assert_eq!(package_dir("caller/sibling.go"), "caller");
        assert_eq!(package_dir("pkg/sub/file.Name"), "pkg/sub");
        // Root-level, no directory at all.
        assert_eq!(package_dir("main.go"), "");
        assert_eq!(package_dir(""), "");
    }

    #[test]
    fn visibility_rule_none_never_refuses() {
        assert!(VisibilityRule::None.is_visible(Some("private"), "a.b", "x.rs", "y.rs", None));
    }

    #[test]
    fn visibility_rule_recorded_refuses_only_explicit_private() {
        let rule = VisibilityRule::Recorded;
        assert!(!rule.is_visible(Some("private"), "a.b", "x.rs", "y.rs", None));
        // `NULL`/unrecorded and any other recorded value stay visible —
        // only an explicit "private" mark refuses.
        assert!(rule.is_visible(None, "a.b", "x.rs", "y.rs", None));
        assert!(rule.is_visible(Some("public"), "a.b", "x.rs", "y.rs", None));
    }

    #[test]
    fn visibility_rule_rust_module_refuses_private_outside_owner_module() {
        let rule = VisibilityRule::RustModule;
        // Unrelated module, no descendant relationship — refused.
        assert!(!rule.is_visible(
            Some("private"),
            "crate::a::helper",
            "a.rs",
            "b.rs",
            Some("crate::b::caller")
        ));
    }

    #[test]
    fn visibility_rule_rust_module_allows_private_from_descendant_module() {
        let rule = VisibilityRule::RustModule;
        // `crate::a::child` is a descendant of `crate::a`, the owning
        // module of the private candidate — visible (issue #75 follow-up,
        // finding D).
        assert!(rule.is_visible(
            Some("private"),
            "crate::a::helper",
            "a.rs",
            "a/child.rs",
            Some("crate::a::child::f")
        ));
        // The owning module itself (not just a descendant) is visible too.
        assert!(rule.is_visible(
            Some("private"),
            "crate::a::helper",
            "a.rs",
            "a/other.rs",
            Some("crate::a::other_fn")
        ));
        // A parent module does *not* inherit visibility into a child's
        // private items — Rust privacy only flows downward.
        assert!(!rule.is_visible(
            Some("private"),
            "crate::a::child::secret",
            "a/child.rs",
            "a.rs",
            Some("crate::a::caller")
        ));
        // No source qualname at all (caller didn't resolve to a symbol) —
        // can't establish descendance, so refused.
        assert!(!rule.is_visible(
            Some("private"),
            "crate::a::helper",
            "a.rs",
            "a/child.rs",
            None
        ));
    }

    #[test]
    fn visibility_rule_rust_module_uses_module_containing_the_impl() {
        let rule = VisibilityRule::RustModule;
        // A private method's owner is the module holding the `impl`
        // (`crate`), not the type segment (`crate::Error`).
        assert!(rule.is_visible(
            Some("private"),
            "crate::Error::with_depth",
            "lib.rs",
            "walk.rs",
            Some("crate::walk::Walk::run")
        ));
        assert!(rule.is_visible(
            Some("private"),
            "crate::Error::with_depth",
            "lib.rs",
            "walk.rs",
            Some("crate::walk::free")
        ));
        // Sibling modules never see each other's private methods.
        assert!(!rule.is_visible(
            Some("private"),
            "crate::a::T::secret",
            "a.rs",
            "b.rs",
            Some("crate::b::U::run")
        ));
    }

    #[test]
    fn visibility_rule_go_capitalization_allows_exported_anywhere() {
        let rule = VisibilityRule::GoCapitalization;
        assert!(rule.is_visible(
            None,
            "other/other.FormatGreeting",
            "other/other.go",
            "caller/caller.go",
            None
        ));
    }

    #[test]
    fn visibility_rule_go_capitalization_allows_unexported_within_same_package_only() {
        let rule = VisibilityRule::GoCapitalization;
        // Same package (directory), different file — the `siblingUtil`
        // shape from the golden Go fixture.
        assert!(rule.is_visible(
            None,
            "caller/sibling.siblingUtil",
            "caller/sibling.go",
            "caller/caller.go",
            None
        ));
        // Different package — refused.
        assert!(!rule.is_visible(
            None,
            "secretpkg/secret.secretUtil",
            "secretpkg/secret.go",
            "prober/prober.go",
            None
        ));
    }

    /// An in-memory, fully migrated connection for `Resolver`-level tests
    /// that need real SQL (ambiguity across two rows, cross-language
    /// filtering) rather than pure-function unit tests.
    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::db::migrations::migrate(&conn).unwrap();
        conn
    }

    fn insert_file(conn: &Connection, path: &str, language: &str) -> i64 {
        conn.execute(
            "INSERT INTO files (path, hash, language, size, modified) VALUES (?, 'h', ?, 0, 0)",
            params![path, language],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn insert_symbol(
        conn: &Connection,
        file_id: i64,
        kind: &str,
        name: &str,
        qualname: &str,
        visibility: Option<&str>,
    ) -> i64 {
        conn.execute(
            "INSERT INTO symbols
                (file_id, kind, name, qualname, start_line, start_col, end_line, end_col,
                 start_byte, end_byte, graph_version, visibility)
             VALUES (?, ?, ?, ?, 0, 0, 0, 0, 0, 0, 1, ?)",
            params![file_id, kind, name, qualname, visibility],
        )
        .unwrap();
        conn.last_insert_rowid()
    }

    fn reference<'a>(
        target_qualname: &'a str,
        edge_kind: &'a str,
        source_lang: &'a str,
        source_file_path: &'a str,
        source_qualname: Option<&'a str>,
        bare_call: bool,
    ) -> Reference<'a> {
        Reference {
            target_qualname: Some(target_qualname),
            edge_kind,
            receiver_type: None,
            receiver_scope: None,
            deferred: None,
            import_candidates: &[],
            source_lang,
            source_file_path,
            source_qualname,
            source_symbol_id: None,
            bare_call,
            call_shape: None,
        }
    }

    /// Issue #75, acceptance criterion 3: the guarded name fallback binds
    /// only with exactly one candidate.
    #[test]
    fn resolve_refuses_ambiguous_guarded_fallback_candidates() {
        let conn = test_conn();
        let file_a = insert_file(&conn, "pkg_a/mod.py", "python");
        let file_b = insert_file(&conn, "pkg_b/mod.py", "python");
        insert_symbol(&conn, file_a, "function", "util", "pkg_a.util", None);
        insert_symbol(&conn, file_b, "function", "util", "pkg_b.util", None);

        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let symbol_map = std::collections::HashMap::new();
        let r = reference("caller.util", "CALLS", "python", "caller.py", None, true);
        let resolution = resolver.resolve(&r, &symbol_map).unwrap();

        assert_eq!(
            resolution,
            Resolution::Unresolved(UnresolvedReason::Ambiguous)
        );
    }

    /// Issue #75, acceptance criterion 4: the guarded name fallback never
    /// crosses languages, except for Bridge Edge kinds.
    #[test]
    fn resolve_never_crosses_languages_except_bridge_edges() {
        let conn = test_conn();
        let py_file = insert_file(&conn, "svc/mod.py", "python");
        insert_symbol(
            &conn,
            py_file,
            "function",
            "shared_name",
            "svc.shared_name",
            None,
        );

        // A non-bridge CALLS edge from Rust must not cross into the
        // Python-only candidate. `crate::`-rooted (like every real Rust
        // extractor guess for a bare/two-segment call -- see
        // `rust::resolve_call_target`), so it doesn't also exercise
        // `is_known_external_fallback`'s tier 6 (issue #80), which only
        // ever fires on the shape a genuine external path keeps: `::`
        // and not `crate::`-rooted (`std::`, a third-party crate, ...).
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let symbol_map = std::collections::HashMap::new();
        let non_bridge = reference(
            "crate::caller::shared_name",
            "CALLS",
            "rust",
            "caller.rs",
            None,
            true,
        );
        assert_eq!(
            resolver.resolve(&non_bridge, &symbol_map).unwrap(),
            Resolution::Unresolved(UnresolvedReason::NoCandidates)
        );

        // The same shape, but a Bridge Edge kind, may cross into it.
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let bridge = reference(
            "crate::caller::shared_name",
            "RPC_CALL",
            "rust",
            "caller.rs",
            None,
            true,
        );
        let resolved = resolver.resolve(&bridge, &symbol_map).unwrap();
        assert!(
            matches!(resolved, Resolution::Resolved { .. }),
            "{resolved:?}"
        );
    }

    /// Issue #75 follow-up (finding G): a truncated candidate set must
    /// never corrupt the ambiguity count. Nine unrelated classes each
    /// declare a `method`-kind `zzq` (decoys the bare-call guard excludes
    /// by kind), and two unrelated modules each declare a `function`-kind
    /// `zzq` (the two candidates a bare, receiver-less `zzq()` call is
    /// genuinely ambiguous between). Before the fix, `SAME_LANG_SQL`'s
    /// `LIMIT 8` truncated the raw (pre-kind-filter) result to 8 rows,
    /// which — in insertion order — held the first function decoy plus
    /// seven of the nine method decoys, cutting the second function
    /// candidate off entirely; kind-filtering the truncated set left
    /// exactly one survivor, so it wrongly bound instead of refusing as
    /// ambiguous.
    #[test]
    fn resolve_refuses_ambiguous_guarded_fallback_candidates_past_the_old_limit() {
        let conn = test_conn();
        // Inserted first, so its row would have sorted first pre-fix too —
        // matches the reported repro shape exactly.
        let fn_a_file = insert_file(&conn, "a_fn.py", "python");
        insert_symbol(&conn, fn_a_file, "function", "zzq", "a_fn.zzq", None);

        for i in 0..9 {
            let file = insert_file(&conn, &format!("c{i}.py"), "python");
            insert_symbol(
                &conn,
                file,
                "method",
                "zzq",
                &format!("c{i}.C{i}.zzq"),
                None,
            );
        }

        let fn_z_file = insert_file(&conn, "z_fn.py", "python");
        insert_symbol(&conn, fn_z_file, "function", "zzq", "z_fn.zzq", None);

        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let symbol_map = std::collections::HashMap::new();
        // Bare call, no receiver: `exclude_method` exempts every
        // `method`-kind decoy, leaving only the two `function`-kind
        // candidates in contention.
        let r = reference("caller.zzq", "CALLS", "python", "caller.py", None, true);
        let resolution = resolver.resolve(&r, &symbol_map).unwrap();

        assert_eq!(
            resolution,
            Resolution::Unresolved(UnresolvedReason::Ambiguous),
            "{resolution:?}"
        );
    }

    /// Issue #110: Case-sensitive matching succeeds with exact match.
    #[test]
    fn resolve_guarded_fallback_succeeds_with_exact_case() {
        let conn = test_conn();
        let file = insert_file(&conn, "pkg/error.py", "python");
        insert_symbol(&conn, file, "class", "Error", "Error", None);

        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let symbol_map = std::collections::HashMap::new();
        let r = reference("Error", "CALLS", "python", "caller.py", None, true);
        let resolution = resolver.resolve(&r, &symbol_map).unwrap();

        assert!(
            matches!(resolution, Resolution::Resolved { .. }),
            "exact case match should resolve: {resolution:?}"
        );
    }

    /// Issue #110: Case-sensitive matching succeeds with dot-separated suffix.
    #[test]
    fn resolve_guarded_fallback_succeeds_with_dot_boundary() {
        let conn = test_conn();
        let file = insert_file(&conn, "pkg/error.py", "python");
        insert_symbol(&conn, file, "class", "Error", "pkg.Error", None);

        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let symbol_map = std::collections::HashMap::new();
        let r = reference("Error", "CALLS", "python", "caller.py", None, true);
        let resolution = resolver.resolve(&r, &symbol_map).unwrap();

        assert!(
            matches!(resolution, Resolution::Resolved { .. }),
            "dot-boundary suffix should resolve: {resolution:?}"
        );
    }

    /// Issue #110: Case-sensitive matching succeeds with :: separator.
    #[test]
    fn resolve_guarded_fallback_succeeds_with_colons_boundary() {
        let conn = test_conn();
        let file = insert_file(&conn, "pkg/error.rs", "rust");
        insert_symbol(&conn, file, "struct", "Error", "crate::error::Error", None);

        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let symbol_map = std::collections::HashMap::new();
        let r = reference("Error", "CALLS", "rust", "caller.rs", None, true);
        let resolution = resolver.resolve(&r, &symbol_map).unwrap();

        assert!(
            matches!(resolution, Resolution::Resolved { .. }),
            ":: boundary suffix should resolve: {resolution:?}"
        );
    }

    /// Issue #110: `SAME_LANG_SQL`'s `LIKE` clauses are case-insensitive
    /// for ASCII, so `new Error()` (bare call, target `caller.Error`) must
    /// not fuzzy-bind to an unrelated, differently-cased `error` function
    /// elsewhere in the index.
    #[test]
    fn resolve_guarded_fallback_is_case_sensitive() {
        let conn = test_conn();
        let file = insert_file(&conn, "pkg/schemas.py", "python");
        insert_symbol(&conn, file, "function", "error", "pkg.schemas.error", None);

        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let symbol_map = std::collections::HashMap::new();
        let r = reference("caller.Error", "CALLS", "python", "caller.py", None, true);
        let resolution = resolver.resolve(&r, &symbol_map).unwrap();

        assert_eq!(
            resolution,
            Resolution::Unresolved(UnresolvedReason::NoCandidates),
            "{resolution:?}"
        );
    }

    /// Same bug, `ANY_LANG_SQL`'s round (Bridge Edge kinds only): a
    /// same-name, different-case candidate in another language must not
    /// bind either.
    #[test]
    fn resolve_any_lang_fallback_is_case_sensitive() {
        let conn = test_conn();
        let py_file = insert_file(&conn, "svc/mod.py", "python");
        insert_symbol(&conn, py_file, "function", "handler", "svc.handler", None);

        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let symbol_map = std::collections::HashMap::new();
        let r = reference(
            "crate::caller::Handler",
            "RPC_CALL",
            "rust",
            "caller.rs",
            None,
            true,
        );
        let resolution = resolver.resolve(&r, &symbol_map).unwrap();

        assert_eq!(
            resolution,
            Resolution::Unresolved(UnresolvedReason::NoCandidates),
            "{resolution:?}"
        );
    }

    #[test]
    fn resolve_any_lang_fallback_refuses_two_candidates() {
        let conn = test_conn();
        let py_file = insert_file(&conn, "svc/mod.py", "python");
        insert_symbol(&conn, py_file, "function", "Handler", "svc.Handler", None);
        let go_file = insert_file(&conn, "other/mod.go", "go");
        insert_symbol(&conn, go_file, "function", "Handler", "other.Handler", None);

        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let symbol_map = std::collections::HashMap::new();
        let r = reference(
            "crate::caller::Handler",
            "RPC_CALL",
            "rust",
            "caller.rs",
            None,
            true,
        );
        let resolution = resolver.resolve(&r, &symbol_map).unwrap();

        assert!(
            matches!(resolution, Resolution::Unresolved(_)),
            "{resolution:?}"
        );
    }

    /// Issue #186: the bridge any-language tier tells C# overloads apart by
    /// the call's arity instead of refusing them as ambiguous.
    #[test]
    fn resolve_any_lang_fallback_is_arity_aware_for_csharp() {
        let conn = test_conn();
        let cs = insert_file(&conn, "Svc.cs", "csharp");
        for sig in ["(int a)", "(int a, int b)"] {
            let id = insert_symbol(&conn, cs, "method", "Handler", "App.Svc.Handler", None);
            conn.execute(
                "UPDATE symbols SET signature = ? WHERE id = ?",
                params![sig, id],
            )
            .unwrap();
        }
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let symbol_map = std::collections::HashMap::new();
        let mut r = reference(
            "crate::caller::Handler",
            "RPC_CALL",
            "rust",
            "caller.rs",
            None,
            true,
        );
        // No arity signal: two overloads, ambiguous.
        assert!(matches!(
            resolver.resolve(&r, &symbol_map).unwrap(),
            Resolution::Unresolved(_)
        ));
        r.call_shape = Some(crate::indexer::extract::CallShape {
            arg_count: 2,
            is_new: false,
            implicit_this: false,
        });
        let resolution = resolver.resolve(&r, &symbol_map).unwrap();
        assert!(
            matches!(resolution, Resolution::Resolved { .. }),
            "{resolution:?}"
        );
    }

    /// Issue #102: a fully-qualified `std::` path never binds to a crate
    /// symbol sharing its trailing name; it stubs as external.
    #[test]
    fn rust_std_path_never_binds_to_crate_symbol() {
        let conn = test_conn();
        let util = insert_file(&conn, "src/util.rs", "rust");
        insert_symbol(
            &conn,
            util,
            "function",
            "read_to_string",
            "crate::util::read_to_string",
            None,
        );
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let map = std::collections::HashMap::new();
        for target in ["std::fs::read_to_string", "core::fmt::read_to_string"] {
            let r = reference(target, "CALLS", "rust", "src/init.rs", None, false);
            let resolution = resolver.resolve(&r, &map).unwrap();
            assert!(
                matches!(
                    resolution,
                    Resolution::Resolved {
                        kind: ResolutionKind::External { .. },
                        ..
                    }
                ),
                "{target}: {resolution:?}"
            );
        }
    }

    /// Issue #102: `Type::assoc()` with `Type` not in the repo never binds
    /// to another type's same-named associated fn.
    #[test]
    fn rust_foreign_type_assoc_never_binds_to_other_types_method() {
        let conn = test_conn();
        let f = insert_file(&conn, "src/db.rs", "rust");
        insert_symbol(&conn, f, "method", "new", "crate::db::Db::new", None);
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let map = std::collections::HashMap::new();
        let r = reference("String::new", "CALLS", "rust", "src/init.rs", None, false);
        let resolution = resolver.resolve(&r, &map).unwrap();
        assert!(
            matches!(
                resolution,
                Resolution::Resolved {
                    kind: ResolutionKind::External { .. },
                    ..
                }
            ),
            "{resolution:?}"
        );
    }

    /// Issue #102: a tuple-variant constructor (`Ok(..)`) never binds by
    /// bare name to a same-named (differing only in case) function.
    #[test]
    fn rust_variant_constructor_never_binds_by_bare_name() {
        let conn = test_conn();
        let f = insert_file(&conn, "src/shadowing.rs", "rust");
        insert_symbol(&conn, f, "function", "ok", "crate::shadowing::ok", None);
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let map = std::collections::HashMap::new();
        let r = reference(
            "crate::db::resolver::Ok",
            "CALLS",
            "rust",
            "src/db/resolver.rs",
            None,
            true,
        );
        let resolution = resolver.resolve(&r, &map).unwrap();
        assert!(
            matches!(resolution, Resolution::Unresolved(_)),
            "{resolution:?}"
        );
    }

    /// Issue #102: symbols under `tests/fixtures/` are never candidates for
    /// a reference from outside them.
    #[test]
    fn fixture_symbols_are_never_candidates_for_src_references() {
        let conn = test_conn();
        let f = insert_file(&conn, "tests/fixtures/golden/rust/src/helper.rs", "rust");
        insert_symbol(
            &conn,
            f,
            "function",
            "helper",
            "crate::helper::helper",
            None,
        );
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let map = std::collections::HashMap::new();
        let r = reference(
            "crate::init::helper",
            "CALLS",
            "rust",
            "src/init.rs",
            None,
            true,
        );
        let resolution = resolver.resolve(&r, &map).unwrap();
        assert!(
            matches!(resolution, Resolution::Unresolved(_)),
            "{resolution:?}"
        );
    }

    /// A `_pb2` candidate is skipped, not decisive: a later candidate
    /// rooted in a repo module still makes the import repo-local.
    #[test]
    fn python_pb2_candidate_before_repo_module_still_counts_as_repo() {
        let conn = test_conn();
        let f = insert_file(&conn, "pkg/__init__.py", "python");
        insert_symbol(&conn, f, "module", "pkg", "pkg", None);
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let candidates = vec!["gen.v1.thing_pb2".to_string(), "pkg.util".to_string()];
        assert!(resolver.is_repo_python_import(&candidates).unwrap());
        let only_pb2 = vec!["pkg.v1.thing_pb2".to_string()];
        assert!(!resolver.is_repo_python_import(&only_pb2).unwrap());
    }

    /// `Type::assoc` where `Type` names two repo types is ambiguous: the
    /// type segment doesn't count as a repo type, so the trailing name
    /// never binds to a same-named repo symbol.
    #[test]
    fn rust_foreign_path_with_ambiguous_type_segment_stays_unresolved() {
        let conn = test_conn();
        let a = insert_file(&conn, "src/a.rs", "rust");
        let b = insert_file(&conn, "src/b.rs", "rust");
        insert_symbol(&conn, a, "struct", "Widget", "crate::a::Widget", None);
        insert_symbol(&conn, b, "struct", "Widget", "crate::b::Widget", None);
        let c = insert_file(&conn, "src/c.rs", "rust");
        insert_symbol(&conn, c, "function", "build", "crate::c::build", None);
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let map = std::collections::HashMap::new();
        let r = reference("Widget::build", "CALLS", "rust", "src/init.rs", None, false);
        let resolution = resolver.resolve(&r, &map).unwrap();
        assert_eq!(
            resolution,
            Resolution::Unresolved(UnresolvedReason::Ambiguous)
        );
    }

    /// Issue #102: a qualname that exists only under `tests/fixtures/` is
    /// never an exact-tier target for a reference from `src/`, but is one
    /// from within fixtures.
    #[test]
    fn exact_tier_ignores_fixture_symbols_from_outside_fixtures() {
        let conn = test_conn();
        let f = insert_file(&conn, "tests/fixtures/golden/rust/src/helper.rs", "rust");
        insert_symbol(
            &conn,
            f,
            "function",
            "helper",
            "crate::helper::helper",
            None,
        );
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let map = std::collections::HashMap::new();
        let r = reference(
            "crate::helper::helper",
            "CALLS",
            "rust",
            "src/init.rs",
            None,
            false,
        );
        assert!(matches!(
            resolver.resolve(&r, &map).unwrap(),
            Resolution::Unresolved(_)
        ));
        let r = reference(
            "crate::helper::helper",
            "CALLS",
            "rust",
            "tests/fixtures/golden/rust/src/caller.rs",
            None,
            false,
        );
        assert!(matches!(
            resolver.resolve(&r, &map).unwrap(),
            Resolution::Resolved {
                kind: ResolutionKind::Exact,
                ..
            }
        ));
    }

    /// Issue #255: both name-fallback queries must seek `idx_symbols_name*`
    /// (never `SCAN s`), the leading-wildcard `LIKE` having no index.
    #[test]
    fn name_fallback_queries_seek_the_name_index() {
        let conn = test_conn();
        for sql in [super::SAME_LANG_SQL, super::ANY_LANG_SQL] {
            let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
            let args =
                (0..stmt.parameter_count()).map(|i| rusqlite::types::Value::Integer(i as i64));
            let plan: Vec<String> = stmt
                .query_map(rusqlite::params_from_iter(args), |row| row.get(3))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            let symbols_step = plan
                .iter()
                .find(|d| d.contains(" s ") || d.ends_with(" s") || d.contains(" s USING"))
                .unwrap_or_else(|| panic!("no step on symbols s: {plan:?}"));
            assert!(
                symbols_step.starts_with("SEARCH") && symbols_step.contains("idx_symbols_name"),
                "name fallback must seek the name index, plan: {plan:?}"
            );
        }
    }

    /// Issue #255: the `name` prefilter is not the decision. A same-named
    /// symbol whose qualname does not end in the looked-up (two-segment)
    /// name is still rejected; exact, `.`-suffix and `::`-suffix matches
    /// still bind.
    #[test]
    fn name_prefilter_still_requires_the_qualname_suffix() {
        let conn = test_conn();
        let py = insert_file(&conn, "pkg/a.py", "python");
        let rs = insert_file(&conn, "src/lib.rs", "rust");
        let exact = insert_symbol(&conn, py, "function", "solo", "solo", None);
        let dotted = insert_symbol(&conn, py, "method", "process", "pkg.Db.process", None);
        insert_symbol(&conn, py, "method", "process", "pkg.Cache.process", None);
        let coloned = insert_symbol(&conn, rs, "function", "new", "crate::db::Db::new", None);
        insert_symbol(
            &conn,
            rs,
            "function",
            "new",
            "crate::cache::Cache::new",
            None,
        );

        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let map = std::collections::HashMap::new();
        let mut id_of = |target: &str, lang: &str| {
            let r = reference(target, "CALLS", lang, "caller", None, false);
            match resolver.resolve(&r, &map).unwrap() {
                Resolution::Resolved { target_id, .. } => Some(target_id),
                _ => None,
            }
        };
        assert_eq!(id_of("solo", "python"), Some(exact));
        assert_eq!(id_of("Db.process", "python"), Some(dotted));
        assert_eq!(id_of("Db::new", "rust"), Some(coloned));
        // `name` matches two symbols, but neither qualname ends in `Other.process`.
        assert_eq!(id_of("Other.process", "python"), None);
    }

    /// Issue #255: with three retained graph versions, only the resolver's
    /// own version's rows are candidates (older copies of the same symbol
    /// neither bind nor make the name ambiguous), and the lookup still seeks
    /// the name index.
    #[test]
    fn name_fallback_ignores_other_graph_versions() {
        let conn = test_conn();
        let file = insert_file(&conn, "pkg/a.py", "python");
        let mut ids = Vec::new();
        for gv in 1..=3 {
            conn.execute(
                "INSERT INTO symbols
                    (file_id, kind, name, qualname, start_line, start_col, end_line, end_col,
                     start_byte, end_byte, graph_version)
                 VALUES (?, 'function', 'util', 'pkg.util', 0, 0, 0, 0, 0, 0, ?)",
                params![file, gv],
            )
            .unwrap();
            ids.push(conn.last_insert_rowid());
        }
        for (gv, id) in (1..=3).zip(&ids) {
            let mut resolver = Resolver::new(&conn, gv).unwrap();
            let map = std::collections::HashMap::new();
            let r = reference("util", "CALLS", "python", "caller.py", None, true);
            let resolution = resolver.resolve(&r, &map).unwrap();
            assert!(
                matches!(resolution, Resolution::Resolved { target_id, .. } if target_id == *id),
                "version {gv}: {resolution:?}"
            );
        }
    }

    /// Issue #255: resolution output is unchanged. For every lookup shape,
    /// the statement `any_lang_lookup` picks (prefiltered, or unfiltered for
    /// non-plain tails) must admit exactly the rows, after the case-sensitive
    /// re-check, that the unfiltered statement does -- over symbols named the
    /// way the extractors emit them (C# `.ctor`, TS computed/quoted keys
    /// included), not derived from the qualname.
    #[test]
    fn name_prefilter_admits_the_same_rows_as_the_unfiltered_predicate() {
        let conn = test_conn();
        let py = insert_file(&conn, "a.py", "python");
        let rs = insert_file(&conn, "a.rs", "rust");
        let cs = insert_file(&conn, "a.cs", "csharp");
        let ts = insert_file(&conn, "a.ts", "typescript");
        let sql = insert_file(&conn, "a.sql", "sql");
        for (f, kind, name, qn) in [
            (py, "function", "run", "a.run"),
            (py, "method", "run", "a.Svc.run"),
            (py, "method", "runner", "a.Svc.runner"),
            (py, "class", "Run", "Run"),
            (rs, "function", "run", "crate::a::run"),
            (rs, "method", "run", "crate::a::Svc::run"),
            (rs, "function", "run", "run"),
            (cs, "method", "Run", "N.Svc.Run"),
            (cs, "method", "Run", "N.Svc.IA.Run"),
            (cs, "method", ".ctor", "N.Svc..ctor"),
            (cs, "method", ".cctor", "N.Svc..cctor"),
            (ts, "method", "[Symbol.iterator]", "K.[Symbol.iterator]"),
            (ts, "method", "'a.b'", "K.'a.b'"),
            (ts, "method", "run", "K.run"),
            (sql, "function", "[run]", "[dbo].[run]"),
        ] {
            insert_symbol(&conn, f, kind, name, qn, None);
        }
        let any = |tail_ok: bool, n: &str, d: &str, c: &str, tail: &str| -> Vec<i64> {
            let text = if tail_ok {
                super::ANY_LANG_SQL.to_string()
            } else {
                super::ANY_LANG_SQL.replace(super::NAME_PREFILTER, "")
            };
            let mut stmt = conn.prepare(&text).unwrap();
            let named = rusqlite::named_params! {
                ":tail": tail, ":name": n, ":p1": d, ":p2": c, ":gv": 1i64,
            };
            let args: &[_] = if tail_ok { named } else { &named[1..] };
            let mut v: Vec<i64> = stmt
                .query_map(args, |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
                .unwrap()
                .map(Result::unwrap)
                .filter(|(_, qn)| super::matches_name_case_sensitive(qn, n))
                .map(|(id, _)| id)
                .collect();
            v.sort();
            v
        };
        let mut non_empty = 0;
        for target in [
            "run",
            "Run",
            "a.run",
            "Svc.run",
            "Svc::run",
            "Svc.Run",
            "IA.Run",
            "[run]",
            "nope",
            "ctor",
            "cctor",
            ".ctor",
            "Svc..ctor",
            "K.[Symbol.iterator]",
            "K.'a.b'",
            "iterator]",
        ] {
            let (name, dot, colons) = super::fuzzy_qualname_patterns(target);
            let two = super::two_segment_qualname_patterns(target);
            let (n2, d2, c2) =
                two.unwrap_or_else(|| (name.to_string(), dot.clone(), colons.clone()));
            for (n, d, c) in [(name.to_string(), dot, colons), (n2, d2, c2)] {
                let tail = super::qualname_trailing_name(&n);
                let expected = any(false, &n, &d, &c, tail);
                let actual = any(super::name_prefilter_applies(tail), &n, &d, &c, tail);
                assert_eq!(expected, actual, "target {target:?}, lookup {n:?}");
                non_empty += usize::from(!expected.is_empty());
            }
        }
        assert!(non_empty >= 8, "fixture should exercise real matches");
    }

    /// Issue #255: the two-pass rule survives the rewrite -- a same-language
    /// round never binds a cross-language candidate (a Rust `CALLS` to a
    /// name only C# declares, constructor included, stays unresolved), while
    /// a bridge edge kind still crosses.
    #[test]
    fn name_fallback_cross_language_false_positive_still_rejected() {
        let conn = test_conn();
        let cs = insert_file(&conn, "a.cs", "csharp");
        let id = insert_symbol(&conn, cs, "method", "Process", "N.Svc.Process", None);
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let map = std::collections::HashMap::new();
        let call = reference("crate::x::Process", "CALLS", "rust", "b.rs", None, false);
        assert!(matches!(
            resolver.resolve(&call, &map).unwrap(),
            Resolution::Unresolved(_)
        ));
        let bridge = reference("crate::x::Process", "RPC_CALL", "rust", "b.rs", None, false);
        assert!(matches!(
            resolver.resolve(&bridge, &map).unwrap(),
            Resolution::Resolved { target_id, .. } if target_id == id
        ));
    }

    /// Issue #255: C# constructors (`name = ".ctor"`) and TS computed-key
    /// methods (`[Symbol.iterator]`) stay name-fallback candidates.
    #[test]
    fn name_fallback_keeps_ctor_and_computed_key_candidates() {
        let conn = test_conn();
        let cs = insert_file(&conn, "a.cs", "csharp");
        let ts = insert_file(&conn, "a.ts", "typescript");
        let ctor = insert_symbol(&conn, cs, "method", ".ctor", "N.Svc..ctor", None);
        let iter = insert_symbol(
            &conn,
            ts,
            "method",
            "[Symbol.iterator]",
            "K.[Symbol.iterator]",
            None,
        );
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let map = std::collections::HashMap::new();
        let mut id_of = |target: &str, lang: &str| {
            let r = reference(target, "CALLS", lang, "caller", None, false);
            match resolver.resolve(&r, &map).unwrap() {
                Resolution::Resolved { target_id, .. } => Some(target_id),
                _ => None,
            }
        };
        assert_eq!(id_of("N.Svc..ctor", "csharp"), Some(ctor));
        assert_eq!(id_of("K.[Symbol.iterator]", "typescript"), Some(iter));
    }
}
