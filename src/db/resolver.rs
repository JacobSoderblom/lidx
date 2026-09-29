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
    CallShape, DEFERRED_ARG_PREFIX, DEFERRED_MARKER_PREFIX, DeferredArgument, DeferredReturn,
    ReceiverType,
};
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, Statement, ToSql, params};
use std::collections::HashMap;
use std::sync::LazyLock;

/// An edge's target as the extractor saw it: what the resolver binds.
pub(crate) struct Reference<'a> {
    /// The call site's target text, e.g. `store.append` or `Db::new`.
    pub target_qualname: Option<&'a str>,
    /// The Edge Kind (`CALLS`, `RPC_CALL`, ...); gates cross-language lookup.
    pub edge_kind: &'a str,
    /// The `edges.receiver_type` column value (see `ReceiverType::as_column`).
    pub receiver_type: Option<&'a str>,
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
    /// `EdgeInput::bare_call` — true for a genuinely bare identifier call
    /// (`foo()`), gating the guarded name-fallback tier's method-kind
    /// exclusion. Meaningless (ignored) for any `edge_kind` other than
    /// `CALLS`.
    pub bare_call: bool,
    /// `EdgeInput::call_shape` -- arity for overload selection and the
    /// `new T(...)` class-to-constructor refinement (issues #123/#124).
    pub call_shape: Option<CallShape>,
}

/// How a resolved target was found. `as_str` is the `edges.resolution_kind`
/// column value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResolutionKind {
    Exact,
    Import,
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
pub(crate) const ALL_RESOLUTION_KINDS: [&str; 7] = [
    ResolutionKind::Exact.as_str(),
    ResolutionKind::Import.as_str(),
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
}

impl UnresolvedReason {
    /// The `unresolved_references.reason` column value (issue #78).
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::NoCandidates => "no_candidates",
            Self::Ambiguous => "ambiguous",
            Self::Private => "private",
            Self::External => "external",
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
    pub(crate) fn stored_receiver_type(self, extracted: Option<&str>) -> Option<&str> {
        match self {
            // A deferred receiver keeps its marker: it is re-judged whenever
            // its callee changes, so it must never be frozen to `""`.
            Self::Resolved {
                kind:
                    ResolutionKind::External {
                        via_language_fallback: false,
                    },
                ..
            } if !extracted.is_some_and(ReceiverType::is_deferred_column) => Some(""),
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

/// `LanguageProfile::parameter_type`: (callee signature, argument index,
/// argument name) -> parameter type name.
pub(crate) type ParameterTypeFn = fn(&str, usize, Option<&str>) -> Option<String>;

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
    /// The receiver type name a call yields, from the callee's indexed
    /// signature and whether the call was awaited (`ReceiverType::Deferred`).
    /// `None` for a language that never defers a receiver.
    pub return_receiver: Option<fn(signature: &str, awaited: bool) -> Option<String>>,
    /// The type name of the parameter at `index` (or named `name`) of a
    /// callee's indexed signature, for a deferred argument marker
    /// (`ReceiverType::deferred_argument`). `None` for a language that
    /// never defers an argument.
    pub parameter_type: Option<ParameterTypeFn>,
}

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
        return_receiver: None,
        parameter_type: None,
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
                    || is_descendant_rust_module(candidate_qualname, source_qualname)
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
fn is_private(visibility: Option<&str>) -> bool {
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
fn is_descendant_rust_module(candidate_qualname: &str, source_qualname: Option<&str>) -> bool {
    let Some(source_qualname) = source_qualname else {
        return false;
    };
    let Some(owner_module) = qualname_container(candidate_qualname) else {
        return false;
    };
    let caller_module = qualname_container(source_qualname).unwrap_or(source_qualname);
    caller_module == owner_module || caller_module.starts_with(&format!("{owner_module}::"))
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
const SAME_LANG_SQL: &str = "SELECT s.id, s.visibility, s.qualname, f.path, s.kind, s.signature
     FROM symbols s
     JOIN files f ON s.file_id = f.id
     WHERE (s.qualname = ? OR s.qualname LIKE ? OR s.qualname LIKE ?)
       AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
       AND (? = 0 OR s.kind != 'method')
       AND s.graph_version = ?
       AND (f.deleted_version IS NULL OR f.deleted_version > ?)
       AND (CASE WHEN f.language IN ('typescript', 'tsx') THEN 'javascript' ELSE f.language END) = ?
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
     WHERE (s.qualname = ? OR s.qualname LIKE ? OR s.qualname LIKE ?)
       AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
       AND s.graph_version = ?
       AND (f.deleted_version IS NULL OR f.deleted_version > ?)";

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
}

/// Resolves references against one graph version. Prepared statements
/// borrow `conn`, so build one per transaction.
pub(crate) struct Resolver<'c> {
    conn: &'c Connection,
    graph_version: i64,
    exact: Statement<'c>,
    same_lang: Statement<'c>,
    any_lang: Statement<'c>,
    hierarchy: Statement<'c>,
    import_suffix: Statement<'c>,
    repo_python_module: Statement<'c>,
    module_exact: Statement<'c>,
    /// Set when any tier of the current `resolve` saw 2+ candidates.
    saw_ambiguous: bool,
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
    /// Lazily resolved `files.id` of the single synthetic external
    /// pseudo-file every stub symbol belongs to (issue #80) -- see
    /// `external_file_id`. `None` until the first stub of this `Resolver`
    /// instance's lifetime is created or looked up.
    external_file_id: Option<i64>,
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
            hierarchy: conn.prepare(HIERARCHY_SQL)?,
            import_suffix: conn.prepare(IMPORT_SUFFIX_SQL)?,
            repo_python_module: conn.prepare(REPO_PYTHON_MODULE_SQL)?,
            module_exact: conn.prepare(MODULE_EXACT_SQL)?,
            saw_ambiguous: false,
            saw_private: false,
            arity: None,
            call_receiver: None,
            external_file_id: None,
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
        if let Some(arg) = r
            .receiver_type
            .and_then(ReceiverType::parse_deferred_argument)
        {
            return Ok(match self.deferred_argument_type(&arg, r.source_lang)? {
                Some(ty) => self.resolve(
                    &Reference {
                        target_qualname: Some(&ty),
                        receiver_type: None,
                        ..*r
                    },
                    symbol_map,
                )?,
                None => Resolution::Unresolved(UnresolvedReason::NoCandidates),
            });
        }
        // A deferred receiver (`ReceiverType::Deferred`) becomes the callee's
        // declared return type, or `""` (unresolved) -- never a guess.
        let deferred;
        let patched;
        let r = match r
            .receiver_type
            .and_then(ReceiverType::parse_deferred_return)
        {
            Some(call) => {
                deferred = self.deferred_receiver(&call, r.source_lang)?;
                patched = Reference {
                    receiver_type: Some(&deferred),
                    ..*r
                };
                &patched
            }
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
        let resolution = self.resolve_tiers(r, symbol_map)?;
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
        Ok(resolution)
    }

    /// The repo type a deferred argument constructs: the declared type of the
    /// parameter it is passed for, shared by every arity-admitted overload of
    /// the callee (differing or unreadable parameters -> `None`, never a
    /// guess) and naming a repo type (which rules out a type parameter).
    fn deferred_argument_type(
        &self,
        arg: &DeferredArgument<'_>,
        lang: &str,
    ) -> Result<Option<String>> {
        let Some(parameter_type) = profile_for(lang).parameter_type else {
            return Ok(None);
        };
        let mut stmt = self.conn.prepare_cached(
            "SELECT s.signature FROM symbols s JOIN files f ON s.file_id = f.id
             WHERE s.kind = 'method' AND f.language = ?4
               AND (s.qualname = ?1 OR substr(s.qualname, -length(?2)) = ?2)
               AND s.graph_version = ?3
               AND (f.deleted_version IS NULL OR f.deleted_version > ?3)",
        )?;
        let suffix = format!(".{}", arg.callee);
        let rows = stmt.query_map(
            params![arg.callee, suffix, self.graph_version, lang],
            |row| row.get::<_, Option<String>>(0),
        )?;
        let arity = Some(Arity {
            args: arg.arg_count,
            value_receiver: false,
        });
        let mut found: Option<String> = None;
        for row in rows {
            let Some(sig) = row? else {
                return Ok(None);
            };
            if !arity_admits(arity, "method", Some(&sig)) {
                continue;
            }
            let Some(ty) = parameter_type(&sig, arg.index, arg.name) else {
                return Ok(None);
            };
            match &found {
                Some(prev) if *prev != ty => return Ok(None),
                _ => found = Some(ty),
            }
        }
        let Some(ty) = found else {
            return Ok(None);
        };
        Ok(self.is_repo_type(&ty, lang)?.then_some(ty))
    }

    /// Whether a class-like symbol named `name` exists in `lang`.
    fn is_repo_type(&self, name: &str, lang: &str) -> Result<bool> {
        Ok(self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM symbols s JOIN files f ON s.file_id = f.id
             WHERE s.name = ?1 AND s.kind IN ('class', 'struct', 'interface', 'record', 'enum')
               AND f.language = ?2 AND s.graph_version = ?3
               AND (f.deleted_version IS NULL OR f.deleted_version > ?3))",
            params![name, lang, self.graph_version],
            |row| row.get(0),
        )?)
    }

    /// The receiver type of `ty.method(..)`'s return value: the return type
    /// (via the language's `return_receiver`) shared by every method named
    /// `method` on a type named `ty` (overloads, same-named types in other
    /// namespaces), else `""`. A `static_only` call needs every candidate to
    /// be `static`, and the return type must name a repo type -- which also
    /// rules out a generic type parameter such as `T`.
    fn deferred_receiver(&self, call: &DeferredReturn<'_>, lang: &str) -> Result<String> {
        let Some(return_receiver) = profile_for(lang).return_receiver else {
            return Ok(String::new());
        };
        let mut stmt = self.conn.prepare_cached(
            "SELECT s.signature, s.visibility FROM symbols s JOIN files f ON s.file_id = f.id
             WHERE s.name = ?1 AND s.kind = 'method' AND f.language = ?5
               AND (s.qualname = ?2 OR substr(s.qualname, -length(?3)) = ?3)
               AND s.graph_version = ?4
               AND (f.deleted_version IS NULL OR f.deleted_version > ?4)",
        )?;
        let suffix = format!(".{}.{}", call.type_name, call.method);
        let qualname = format!("{}.{}", call.type_name, call.method);
        let rows = stmt.query_map(
            params![call.method, qualname, suffix, self.graph_version, lang],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<String>>(1)?,
                ))
            },
        )?;
        let mut found: Option<String> = None;
        for row in rows {
            let (sig, visibility) = row?;
            let is_static = visibility
                .as_deref()
                .is_some_and(|v| v.split_whitespace().any(|m| m == "static"));
            let ret = sig.and_then(|sig| return_receiver(&sig, call.awaited));
            let Some(ret) = ret.filter(|_| is_static || !call.static_only) else {
                return Ok(String::new());
            };
            match &found {
                Some(prev) if *prev != ret => return Ok(String::new()),
                _ => found = Some(ret),
            }
        }
        let Some(ret) = found else {
            return Ok(String::new());
        };
        let is_repo_type = self.is_repo_type(&ret, lang)?;
        Ok(if is_repo_type { ret } else { String::new() })
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
        if !matches!(kind.as_str(), "class" | "struct" | "record") || signature.is_some() {
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

        if let Some(qn) = r.target_qualname
            && let Some(id) = self.exact(
                qn,
                symbol_map,
                r.source_file_path,
                matches!(r.edge_kind, "IMPLEMENTS" | "EXTENDS" | "INHERITS"),
            )?
        {
            return Ok(resolved(id, ResolutionKind::Exact));
        }
        if let Some(id) = self.resolve_import(
            r.import_candidates,
            symbol_map,
            r.source_lang,
            r.source_file_path,
        )? {
            return Ok(resolved(id, ResolutionKind::Import));
        }

        // `import_candidates` is populated only when the extractor already
        // established (from this file's own using/import directives) that
        // the receiver is bound by an import, and the import tier just
        // found no single local symbol for it. Whether that refuses the
        // name-based tiers (binding on name-uniqueness alone would risk
        // e.g. `datetime.now()` landing on an unrelated local
        // `FakeClock.now`) or falls through to them is this language's
        // `LanguageProfile::import_miss` policy.
        let refuse_names = !r.import_candidates.is_empty() && {
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

        let found = match r.target_qualname {
            Some(qn) => self.resolve_by_name(
                qn,
                receiver_type,
                r.edge_kind,
                r.source_lang,
                CallerContext {
                    file_path: r.source_file_path,
                    qualname: r.source_qualname,
                },
                r.bare_call,
            )?,
            None => None,
        };
        match found {
            Some((id, kind)) => Ok(resolved(id, kind)),
            // Tier 3, issue #80's actual scope: the receiver is bound by an
            // import known not to resolve here -- "calls into imports known
            // to resolve outside the repo (standard library, third-party
            // packages)".
            None if refuse_names => {
                self.stub_resolution(r.target_qualname, r.import_candidates, r.bare_call, false)
            }
            // A builtin/unresolved receiver type with no import involved at
            // all (a local variable, e.g. `cells = []` then
            // `cells.append(1)`) is not an import known to resolve
            // elsewhere -- stub it and every other such call site would
            // collapse onto one stub named after whichever local variable's
            // call happened to create it, defeating "who calls X?". Stay
            // unresolved instead, as before #80 -- see
            // `UnresolvedReason::External`'s doc.
            None if receiver_type == Some("") => {
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
                    Some(qn) => self.is_known_external_fallback(r.source_lang, qn)?,
                    None => false,
                };
                if is_external {
                    self.stub_resolution(r.target_qualname, r.import_candidates, r.bare_call, true)
                } else {
                    Ok(Resolution::Unresolved(UnresolvedReason::NoCandidates))
                }
            }
        }
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
                self.saw_ambiguous = true;
            }
            return Ok(exactly_one(admitted.into_iter()).map(|c| c.id));
        }
        let resolved = collapse_exact_candidates(&candidates);
        if resolved.is_none() && candidates.len() > 1 {
            self.saw_ambiguous = true;
        }
        Ok(resolved)
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
        let gv = self.graph_version;
        let exclude_method: i64 = guard.exclude_method as i64;
        let same = self.same_lang_lookup(
            params![name, same_p1, same_p2, exclude_method, gv, gv, source_lang],
            guard,
            source_lang,
            caller,
            name,
        )?;
        if same.is_some() || !is_bridge_edge_kind(edge_kind) {
            return Ok(same);
        }
        let (dot_pattern, colons_pattern) = any_lang_patterns;
        self.any_lang_lookup(params![name, dot_pattern, colons_pattern, gv, gv], name)
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
    fn any_lang_lookup(&mut self, query_params: &[&dyn ToSql], name: &str) -> Result<Option<i64>> {
        let arity = self.arity;
        let mut rows = self.any_lang.query(query_params)?;
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
        query_params: &[&dyn ToSql],
        guard: FallbackGuard,
        source_lang: &str,
        caller: CallerContext<'_>,
        name: &str,
    ) -> Result<Option<i64>> {
        let visibility_rule = profile_for(source_lang).visibility;
        let arity = self.arity;
        let mut rows = self.same_lang.query(query_params)?;
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
                let method = qualname_trailing_name(target_qualname);
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
                    .resolve_via_inheritance(known_type, method, source_lang, edge_kind, caller)?
                    .map(|id| (id, ResolutionKind::Inherited)))
            }

            None => {
                // A bare call (no receiver at all, syntactically) can never
                // dispatch to a `method` — only a receiver decides which
                // instance's method runs. Gated to `CALLS` only: other edge
                // kinds (RPC_CALL, HTTP_CALL, CHANNEL_*, XREF, ...) have
                // their own detection and don't set `bare_call`, so this
                // never restricts them (see `EdgeInput::bare_call`).
                let guard = FallbackGuard {
                    exclude_method: edge_kind == "CALLS" && bare_call,
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
        let gv = self.graph_version;
        self.same_lang_lookup(
            params![name, p1, p2, 0i64, gv, gv, source_lang],
            FallbackGuard::NONE,
            source_lang,
            CallerContext {
                file_path: source_file_path,
                qualname: None,
            },
            name,
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
        known_type: &str,
        method: &str,
        source_lang: &str,
        edge_kind: &str,
        caller: CallerContext<'_>,
    ) -> Result<Option<i64>> {
        let Some(root_id) = self.resolve_type_symbol(known_type, source_lang, caller.file_path)?
        else {
            return Ok(None);
        };

        let mut frontier = vec![root_id];
        let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::from([root_id]);

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
        target_qualname: Option<&str>,
        import_candidates: &[String],
        bare_call: bool,
        via_language_fallback: bool,
    ) -> Result<Resolution> {
        match external_stub_qualname(target_qualname, import_candidates, bare_call) {
            Some(qualname) => {
                let stub_id = self.resolve_external_stub(&qualname)?;
                Ok(resolved(
                    stub_id,
                    ResolutionKind::External {
                        via_language_fallback,
                    },
                ))
            }
            None => Ok(Resolution::Unresolved(UnresolvedReason::NoCandidates)),
        }
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
    Some(format!("ext:{text}"))
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
/// call_shape, graph_version. Used by `Db::insert_edges` (an edge's first resolution
/// attempt), `reconcile_unresolved_reference_store` (an edge that went
/// NULL-target with no row yet), and `Db::carry_forward_files` (carrying an
/// already-unresolved reference into the new graph version).
pub(crate) const UNRESOLVED_REFERENCE_INSERT_SQL: &str = "INSERT INTO unresolved_references
     (edge_id, source_symbol_id, file_id, edge_kind, reference_name, name_tail,
      reason, import_candidates, detail, evidence_snippet, evidence_start_line,
      evidence_end_line, confidence, commit_sha, trace_id, span_id, event_ts,
      receiver_type, bare_call, call_shape, graph_version)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

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
    import_candidates: Option<String>,
    bare_call: bool,
    call_shape: Option<String>,
    source_lang: String,
    file_path: String,
    source_qualname: Option<String>,
}

impl ReferenceContext {
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
        resolver.resolve(
            &Reference {
                target_qualname: self.target_qualname.as_deref(),
                edge_kind: &self.edge_kind,
                receiver_type: self.receiver_type.as_deref(),
                import_candidates: &import_candidates,
                source_lang: &self.source_lang,
                source_file_path: &self.file_path,
                source_qualname: self.source_qualname.as_deref(),
                bare_call: self.bare_call,
                call_shape: self.call_shape.as_deref().and_then(CallShape::decode),
            },
            symbol_map,
        )
    }
}

/// The callee text a deferred-argument edge keeps as its `target_qualname`
/// while unbound (`None` for any other edge).
pub(crate) fn deferred_callee(receiver_type: Option<&str>) -> Option<String> {
    ReceiverType::parse_deferred_argument(receiver_type?).map(|arg| arg.callee.to_string())
}

/// The `target_qualname` to store for an edge just bound to `target_id`: the
/// bound symbol's qualname for a deferred-argument edge (whose own text is
/// only its callee's name), else the extracted text unchanged.
pub(crate) fn bound_target_qualname(
    conn: &Connection,
    receiver_type: Option<&str>,
    target_qualname: Option<&str>,
    target_id: i64,
) -> Result<Option<String>> {
    if receiver_type.is_some_and(|r| r.starts_with(DEFERRED_ARG_PREFIX)) {
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
           target_qualname = CASE WHEN receiver_type LIKE '{DEFERRED_ARG_PREFIX}%'
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
        return None;
    }
    let by_kind: Vec<(i64, &str)> = candidates.iter().map(|c| (c.id, c.kind.as_str())).collect();
    same_kind_min(&by_kind)
}

/// One `EXACT_SQL` row.
struct ExactCandidate {
    id: i64,
    file_id: i64,
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
    /// - `Db::carry_forward_files` copies an unchanged file's edges into
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
                        COALESCE(f.language, 'unknown'), f.path, src.qualname, e.call_shape
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
                        import_candidates: row.get(6)?,
                        bare_call: row.get(7)?,
                        call_shape: row.get(20)?,
                        source_lang: row.get(17)?,
                        file_path: row.get(18)?,
                        source_qualname: row.get(19)?,
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
                            deferred_callee(row.ctx.receiver_type.as_deref())
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
    /// definition (see the callers' own docs) -- widens the join for
    /// `reason = 'ambiguous'` rows only, to match against *every* existing
    /// symbol instead of just ones past the watermark, and skips the
    /// cheap-check early return so that widened pass still runs even when
    /// nothing new was inserted. Ambiguity is the only reason a deletion
    /// (rather than an insertion) can unblock: it's the only outcome caused
    /// by *too many* candidates, so removing one can turn it unique again,
    /// and no new `symbols.id` is ever inserted for the watermark to notice
    /// that by. Every other reason (`NoCandidates`, `External`, `Private`)
    /// can only be fixed by something arriving, so those rows keep the
    /// normal watermark-gated join even on a deletion-carrying batch. Still
    /// only re-resolves rows already in the store, so this stays bounded by
    /// the store's size rather than every edge in the graph.
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
    /// pass, its original `reason` unchanged even if a different one would
    /// now apply.
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
        // A stored deferred-receiver row hangs on its callee's signature,
        // not on any symbol sharing its name, so it is always retried.
        let has_deferred_rows: bool = self.read_conn()?.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM unresolved_references
                 WHERE graph_version = ? AND receiver_type LIKE '{DEFERRED_MARKER_PREFIX}%')"
            ),
            params![graph_version],
            |row| row.get(0),
        )?;
        if !symbols_deleted_this_batch
            && !inheritance_changed
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
            let mut stmt = tx.prepare(&format!(
                "SELECT DISTINCT ur.id, ur.edge_id, ur.source_symbol_id, ur.file_id,
                        ur.edge_kind, ur.reference_name, ur.import_candidates,
                        ur.receiver_type, ur.bare_call, ur.detail, ur.evidence_snippet,
                        ur.evidence_start_line, ur.evidence_end_line, ur.confidence,
                        ur.commit_sha, ur.trace_id, ur.span_id, ur.event_ts,
                        COALESCE(f.language, 'unknown'), f.path, src.qualname, ur.call_shape
                 FROM unresolved_references ur
                 JOIN files f ON f.id = ur.file_id
                 LEFT JOIN symbols src ON src.id = ur.source_symbol_id
                 JOIN symbols s ON (s.qualname = ur.reference_name OR s.name = ur.name_tail)
                 WHERE ur.graph_version = ?1
                   AND s.graph_version = ?1
                   AND (s.id > ?2 OR (?3 AND ur.reason = 'ambiguous'))

                 UNION

                 SELECT DISTINCT ur.id, ur.edge_id, ur.source_symbol_id, ur.file_id,
                        ur.edge_kind, ur.reference_name, ur.import_candidates,
                        ur.receiver_type, ur.bare_call, ur.detail, ur.evidence_snippet,
                        ur.evidence_start_line, ur.evidence_end_line, ur.confidence,
                        ur.commit_sha, ur.trace_id, ur.span_id, ur.event_ts,
                        COALESCE(f.language, 'unknown'), f.path, src.qualname, ur.call_shape
                 FROM unresolved_references ur
                 JOIN files f ON f.id = ur.file_id
                 LEFT JOIN symbols src ON src.id = ur.source_symbol_id
                 WHERE ur.graph_version = ?1
                   AND ((?4 AND ur.receiver_type IS NOT NULL AND ur.receiver_type != '')
                        OR ur.receiver_type LIKE '{DEFERRED_MARKER_PREFIX}%')"
            ))?;
            let rows = stmt.query_map(
                params![
                    graph_version,
                    watermark,
                    symbols_deleted_this_batch,
                    inheritance_changed
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
                            bare_call: row.get(8)?,
                            call_shape: row.get(21)?,
                            source_lang: row.get(18)?,
                            file_path: row.get(19)?,
                            source_qualname: row.get(20)?,
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
                  resolution_kind, import_candidates, bare_call, call_shape)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            let mut delete_store = tx.prepare("DELETE FROM unresolved_references WHERE id = ?")?;
            let empty_symbol_map: HashMap<String, i64> = HashMap::new();

            for row in &candidates {
                let resolution = row.ctx.resolve(&mut resolver, &empty_symbol_map)?;
                if let Resolution::Resolved { target_id, kind } = resolution {
                    match row.ctx.edge_id {
                        Some(edge_id) => {
                            update_edge.execute(params![
                                target_id,
                                kind.as_str(),
                                edge_id,
                                deferred_callee(row.ctx.receiver_type.as_deref())
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
                                    row.ctx.receiver_type.as_deref(),
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
            &format!(
                "AND e.target_symbol_id IS NOT NULL
                 AND e.receiver_type LIKE '{DEFERRED_MARKER_PREFIX}%'"
            ),
        )
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
                        COALESCE(f.language, 'unknown'), f.path, src.qualname, e.call_shape
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
                        import_candidates: row.get(5)?,
                        bare_call: row.get(6)?,
                        call_shape: row.get(10)?,
                        source_lang: row.get(7)?,
                        file_path: row.get(8)?,
                        source_qualname: row.get(9)?,
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
                            deferred_callee(row.ctx.receiver_type.as_deref())
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
                            deferred_callee(row.ctx.receiver_type.as_deref())
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
    /// `Db::carry_forward_files` unconditionally copies every stub forward
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
            import_candidates: &[],
            source_lang,
            source_file_path,
            source_qualname,
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
}
