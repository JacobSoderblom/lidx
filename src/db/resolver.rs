//! Edge-target resolution: the one place that decides which symbol an
//! edge points at, and the only producer of `edges.resolution_kind`.
//!
//! `Db::insert_edges` resolves each edge through [`Resolver::resolve`];
//! the NULL-target repair pass (`Db::resolve_null_target_edges`, below)
//! retries the same tiers once more symbols exist. Every SQL candidate
//! lookup lives in this module.
//!
//! Tier order, first hit wins:
//! 1. **exact** — `target_qualname` names a symbol verbatim.
//! 2. **import** — one of the extractor's import-qualified candidates names
//!    exactly one symbol (`resolve_import`).
//! 3. **known-external refusal** — the receiver is bound by an import that
//!    didn't resolve here, or is a builtin/unresolved type: stop, no guess.
//! 4. **receiver type / inheritance** — a known receiver type's own method,
//!    else the closest ancestor declaring it (`resolve_via_inheritance`).
//! 5. **guarded name fallback** — two trailing segments, then the bare
//!    name. Same language only, and only when exactly one candidate
//!    matches.
//!
//! Bridge Edge kinds (see `is_bridge_edge_kind`) may cross languages in
//! tiers 4–5 when the same-language lookup misses.
//!
//! ponytail: this is today's order, kept as-is by the #73 refactor. It
//! differs from the #70 spec order in two ways: there is no same
//! scope/module tier yet, and known-external refusal runs before the name
//! tiers rather than last. Language resolution profiles (`LanguageProfile`)
//! now drive the separator and import-miss-fallback per-language checks
//! this tier order used to hardcode; a same scope/module tier and stricter
//! guards are still open (#75).

use super::Db;
use crate::indexer::channel::is_bridge_edge_kind;
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, Statement, ToSql, params};
use std::collections::HashMap;

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
    /// `EdgeInput::bare_call` — true for a genuinely bare identifier call
    /// (`foo()`), gating the guarded name-fallback tier's method-kind
    /// exclusion. Meaningless (ignored) for any `edge_kind` other than
    /// `CALLS`.
    pub bare_call: bool,
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
}

impl ResolutionKind {
    /// The `edges.resolution_kind` column value.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Import => "import",
            Self::ReceiverType => "receiver_type",
            Self::Inherited => "inherited",
            Self::TwoSegment => "two_segment",
            Self::BareName => "bare_name",
        }
    }
}

/// Why a reference stayed unresolved.
///
/// ponytail: no `CrossLanguageRefused` yet — the guarded name fallback
/// already never crosses languages (`unique_by_pattern`'s same-language
/// round always runs first, and the any-language round is gated by
/// `is_bridge_edge_kind`), so a cross-language miss there just falls
/// through to `NoCandidates`/`Ambiguous` rather than needing its own
/// reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnresolvedReason {
    /// No tier found any candidate.
    NoCandidates,
    /// Some tier found more than one candidate and refused to pick.
    Ambiguous,
    /// The receiver is bound by an import that doesn't resolve uniquely in
    /// this index (stdlib, third-party, or an ambiguous import), or is a
    /// builtin/unresolved type. The name-based tiers are refused outright.
    External,
    /// The guarded name fallback (tier 5) found a same-language, same-kind
    /// candidate by name, but every one of them was private/unexported and
    /// in a different file (or, for Go, a different package) than the
    /// reference — see `VisibilityRule`. Only this tier checks visibility;
    /// an exact/import/receiver-type/inherited match never refuses on it.
    Private,
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
    /// extractor reported `extracted`. An `External` refusal is stored as
    /// `""` so the repair pass never runs the name-based tiers on it either.
    pub(crate) fn stored_receiver_type(self, extracted: Option<&str>) -> Option<&str> {
        match self {
            Self::Unresolved(UnresolvedReason::External) => Some(""),
            _ => extracted,
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
    /// How the guarded name-fallback tier (`Resolver::same_lang_lookup`,
    /// tier 5 only — see the module doc) decides whether a same-language,
    /// same-kind, cross-file candidate is visible to the reference. Never
    /// consulted by the exact/import/receiver-type/inherited tiers.
    pub visibility: VisibilityRule,
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
        visibility: VisibilityRule::None,
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
    /// meaning "no modifier recorded") is visible. Rust (`pub`), C# and
    /// TypeScript/JavaScript (`private`).
    Recorded,
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
    fn is_visible(
        self,
        candidate_visibility: Option<&str>,
        candidate_qualname: &str,
        candidate_file_path: &str,
        source_file_path: &str,
    ) -> bool {
        match self {
            VisibilityRule::None => true,
            VisibilityRule::Recorded => candidate_visibility != Some("private"),
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

/// Same-language fuzzy candidates. `LIMIT 8`, not 1: `Resolver::unique`/
/// `Resolver::same_lang_lookup` need to see every extra row (bounded) to
/// know a match is ambiguous, and `same_lang_lookup` additionally filters
/// some rows out (bare-call method exclusion, visibility) before making
/// that call — see its doc. The language `CASE` must agree with
/// `resolution_language_family`. The extra columns past `s.id` are read
/// only by `same_lang_lookup`; `resolve_type_symbol`'s plain `unique` call
/// (tiers 4's type-name lookups, which don't filter) ignores them.
const SAME_LANG_SQL: &str = "SELECT s.id, s.kind, s.visibility, s.qualname, f.path
     FROM symbols s
     JOIN files f ON s.file_id = f.id
     WHERE (s.qualname = ? OR s.qualname LIKE ? OR s.qualname LIKE ?)
       AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
       AND s.graph_version = ?
       AND (f.deleted_version IS NULL OR f.deleted_version > ?)
       AND (CASE WHEN f.language IN ('typescript', 'tsx') THEN 'javascript' ELSE f.language END) = ?
     LIMIT 8";

/// Cross-language fuzzy candidates, for Bridge Edge kinds only.
const ANY_LANG_SQL: &str = "SELECT s.id
     FROM symbols s
     JOIN files f ON s.file_id = f.id
     WHERE (s.qualname = ? OR s.qualname LIKE ? OR s.qualname LIKE ?)
       AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
       AND s.graph_version = ?
       AND (f.deleted_version IS NULL OR f.deleted_version > ?)
     LIMIT 2";

/// A type's recorded EXTENDS/IMPLEMENTS/INHERITS edges in declaration
/// order, for `resolve_via_inheritance`. `edges.id` is insertion order,
/// which mirrors source order — each extractor emits a class's base-list
/// edges in one pass, in the order the bases are written.
const HIERARCHY_SQL: &str = "SELECT target_symbol_id, target_qualname
     FROM edges
     WHERE source_symbol_id = ?
       AND kind IN ('EXTENDS', 'IMPLEMENTS', 'INHERITS')
       AND graph_version = ?
       AND target_qualname IS NOT NULL
     ORDER BY id ASC";

const EXACT_SQL: &str =
    "SELECT id FROM symbols WHERE qualname = ? AND graph_version = ? ORDER BY id ASC LIMIT 1";

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

/// Which prepared candidate query `Resolver::unique` runs.
#[derive(Clone, Copy)]
enum Lookup {
    SameLang,
    AnyLang,
    ImportSuffix,
}

/// Resolves references against one graph version. Prepared statements
/// borrow `conn`, so build one per transaction.
pub(crate) struct Resolver<'c> {
    graph_version: i64,
    exact: Statement<'c>,
    same_lang: Statement<'c>,
    any_lang: Statement<'c>,
    hierarchy: Statement<'c>,
    import_suffix: Statement<'c>,
    repo_python_module: Statement<'c>,
    /// Set when any tier of the current `resolve` saw 2+ candidates.
    saw_ambiguous: bool,
    /// Set when `same_lang_lookup` (the guarded name-fallback tier only)
    /// found same-language, same-kind candidate(s) by name but refused
    /// every one of them as not visible — see `VisibilityRule`.
    saw_private: bool,
}

impl<'c> Resolver<'c> {
    /// Prepare every candidate query against `conn` for `graph_version`.
    pub(crate) fn new(conn: &'c Connection, graph_version: i64) -> Result<Self> {
        Ok(Self {
            graph_version,
            exact: conn.prepare(EXACT_SQL)?,
            same_lang: conn.prepare(SAME_LANG_SQL)?,
            any_lang: conn.prepare(ANY_LANG_SQL)?,
            hierarchy: conn.prepare(HIERARCHY_SQL)?,
            import_suffix: conn.prepare(IMPORT_SUFFIX_SQL)?,
            repo_python_module: conn.prepare(REPO_PYTHON_MODULE_SQL)?,
            saw_ambiguous: false,
            saw_private: false,
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
        self.saw_ambiguous = false;
        self.saw_private = false;

        if let Some(qn) = r.target_qualname
            && let Some(id) = self.exact(qn, symbol_map)?
        {
            return Ok(resolved(id, ResolutionKind::Exact));
        }
        if let Some(id) = self.resolve_import(r.import_candidates, symbol_map, r.source_lang)? {
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
                r.source_file_path,
                r.bare_call,
            )?,
            None => None,
        };
        Ok(match found {
            Some((id, kind)) => resolved(id, kind),
            None if receiver_type == Some("") => Resolution::Unresolved(UnresolvedReason::External),
            None if self.saw_ambiguous => Resolution::Unresolved(UnresolvedReason::Ambiguous),
            None if self.saw_private => Resolution::Unresolved(UnresolvedReason::Private),
            None => Resolution::Unresolved(UnresolvedReason::NoCandidates),
        })
    }

    fn exact(&mut self, qualname: &str, symbol_map: &HashMap<String, i64>) -> Result<Option<i64>> {
        if let Some(&id) = symbol_map.get(qualname) {
            return Ok(Some(id));
        }
        Ok(self
            .exact
            .query_row(params![qualname, self.graph_version], |row| row.get(0))
            .optional()?)
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
            Lookup::SameLang => &mut self.same_lang,
            Lookup::AnyLang => &mut self.any_lang,
            Lookup::ImportSuffix => &mut self.import_suffix,
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
    /// language round never applies `exclude_method`/`enforce_visibility`:
    /// those guards are specific to the guarded, same-language-only
    /// fallback (see the module doc and issue #75); a Bridge Edge crossing
    /// languages already has its own, separate cross-language contract.
    #[allow(clippy::too_many_arguments)]
    fn unique_by_pattern(
        &mut self,
        name: &str,
        any_lang_patterns: (&str, &str),
        source_lang: &str,
        edge_kind: &str,
        exclude_method: bool,
        enforce_visibility: bool,
        source_file_path: &str,
    ) -> Result<Option<i64>> {
        let profile = profile_for(source_lang);
        let (same_p1, same_p2) = same_lang_patterns(name, &profile);
        let gv = self.graph_version;
        let same = self.same_lang_lookup(
            params![name, same_p1, same_p2, gv, gv, source_lang],
            exclude_method,
            enforce_visibility,
            source_lang,
            source_file_path,
        )?;
        if same.is_some() || !is_bridge_edge_kind(edge_kind) {
            return Ok(same);
        }
        let (dot_pattern, colons_pattern) = any_lang_patterns;
        self.unique(
            Lookup::AnyLang,
            params![name, dot_pattern, colons_pattern, gv, gv],
        )
    }

    /// The guarded name-fallback tier's same-language candidate lookup
    /// (tier 5 only — see the module doc): runs `SAME_LANG_SQL`, then
    /// applies, in order, the bare-call method exclusion and the
    /// language's `VisibilityRule`, and finally the one-candidate guard
    /// over whatever survives both.
    ///
    /// `exclude_method`/`enforce_visibility` are `false` for every other
    /// caller (`resolve_type_symbol`'s type-name lookups, reached from
    /// tier 4's receiver-type/inherited resolution) — those still go
    /// through `unique(Lookup::SameLang, ...)` unchanged, which reads only
    /// this query's first column and applies no filtering: unmatched
    /// same-language rows made this tier ambiguous before #75 and still
    /// do.
    ///
    /// A same-file candidate is always visible, regardless of
    /// `enforce_visibility` — a symbol is never private to its own file.
    /// When nothing survives filtering but at least one same-name,
    /// same-kind row existed before filtering, that's specifically a
    /// privacy refusal (`saw_private`), not a plain miss — `resolve`
    /// reports `Unresolved(Private)` for it, distinct from
    /// `NoCandidates`/`Ambiguous`.
    fn same_lang_lookup(
        &mut self,
        query_params: &[&dyn ToSql],
        exclude_method: bool,
        enforce_visibility: bool,
        source_lang: &str,
        source_file_path: &str,
    ) -> Result<Option<i64>> {
        struct Candidate {
            id: i64,
            kind: String,
            visibility: Option<String>,
            qualname: String,
            file_path: String,
        }

        let rows: Vec<Candidate> = {
            let mut rows = self.same_lang.query(query_params)?;
            let mut out = Vec::new();
            while let Some(row) = rows.next()? {
                out.push(Candidate {
                    id: row.get(0)?,
                    kind: row.get(1)?,
                    visibility: row.get(2)?,
                    qualname: row.get(3)?,
                    file_path: row.get(4)?,
                });
            }
            out
        };

        let visibility_rule = profile_for(source_lang).visibility;
        let mut kind_eligible = 0usize;
        let mut visible: Vec<i64> = Vec::new();
        for c in &rows {
            if exclude_method && c.kind == "method" {
                continue;
            }
            kind_eligible += 1;
            let ok = !enforce_visibility
                || c.file_path == source_file_path
                || visibility_rule.is_visible(
                    c.visibility.as_deref(),
                    &c.qualname,
                    &c.file_path,
                    source_file_path,
                );
            if ok {
                visible.push(c.id);
            }
        }

        match visible.len() {
            1 => Ok(Some(visible[0])),
            0 if kind_eligible > 0 => {
                self.saw_private = true;
                Ok(None)
            }
            0 => Ok(None),
            _ => {
                self.saw_ambiguous = true;
                Ok(None)
            }
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
    /// `source_file_path` and `bare_call` feed the guarded fallback's
    /// (tier 5, the `None` arm) visibility and bare-call guards only —
    /// tier 4 (the `Some` arm) passes `exclude_method`/`enforce_visibility`
    /// as `false` and never consults either.
    #[allow(clippy::too_many_arguments)]
    fn resolve_by_name(
        &mut self,
        target_qualname: &str,
        receiver_type: Option<&str>,
        edge_kind: &str,
        source_lang: &str,
        source_file_path: &str,
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
                    false,
                    false,
                    source_file_path,
                )? {
                    return Ok(Some((id, ResolutionKind::ReceiverType)));
                }
                // The receiver's own type declares no matching method (or
                // the match there was itself ambiguous) — walk its ancestors.
                Ok(self
                    .resolve_via_inheritance(
                        known_type,
                        method,
                        source_lang,
                        edge_kind,
                        source_file_path,
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
                let exclude_method = edge_kind == "CALLS" && bare_call;
                if let Some((seg, dot, colons)) = two_segment_qualname_patterns(target_qualname)
                    && let Some(id) = self.unique_by_pattern(
                        &seg,
                        (&dot, &colons),
                        source_lang,
                        edge_kind,
                        exclude_method,
                        true,
                        source_file_path,
                    )?
                {
                    return Ok(Some((id, ResolutionKind::TwoSegment)));
                }
                let (name, dot, colons) = fuzzy_qualname_patterns(target_qualname);
                Ok(self
                    .unique_by_pattern(
                        name,
                        (&dot, &colons),
                        source_lang,
                        edge_kind,
                        exclude_method,
                        true,
                        source_file_path,
                    )?
                    .map(|id| (id, ResolutionKind::BareName)))
            }
        }
    }

    /// Resolve a bare type name (a receiver's inferred type, or an
    /// ancestor's `target_qualname` text) to the single symbol declaring
    /// it. Same-language only — a class hierarchy never crosses languages.
    fn resolve_type_symbol(&mut self, type_name: &str, source_lang: &str) -> Result<Option<i64>> {
        let name = qualname_trailing_name(type_name);
        let (p1, p2) = same_lang_patterns(name, &profile_for(source_lang));
        let gv = self.graph_version;
        self.unique(Lookup::SameLang, params![name, p1, p2, gv, gv, source_lang])
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
        source_file_path: &str,
    ) -> Result<Option<i64>> {
        let Some(root_id) = self.resolve_type_symbol(known_type, source_lang)? else {
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
                    false,
                    false,
                    source_file_path,
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
                    self.exact(candidate, symbol_map)?
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
}

fn resolved(target_id: i64, kind: ResolutionKind) -> Resolution {
    Resolution::Resolved { target_id, kind }
}

/// One `resolve_null_target_edges` pass-3 row: everything `Resolver::
/// resolve_by_name` needs for one unresolved edge. A named struct rather
/// than a tuple, since it's wide enough to trip `clippy::type_complexity`.
struct UnresolvedNameRow {
    edge_id: i64,
    target_qualname: String,
    source_lang: String,
    edge_kind: String,
    receiver_type: Option<String>,
    file_path: String,
    has_receiver: bool,
}

impl Db {
    /// Repair pass: retry resolution for edges whose target is still NULL,
    /// now that more symbols may exist (e.g. after an incremental reindex
    /// carried unchanged files forward — fresh files' edges are inserted
    /// *before* that, see `Indexer::reindex`). Three passes, same tier
    /// order as `Resolver::resolve`:
    /// 1. exact, as one bulk UPDATE;
    /// 2. the import tier, for rows with `import_candidates`;
    /// 3. the name-based tiers, for rows not refused as external
    ///    (`receiver_type = ''`).
    ///
    /// Processing is done in batches of 1000 rows to avoid long lock holds.
    ///
    /// ponytail: pass 2 only retries edges whose `import_candidates` column
    /// is non-NULL, i.e. ones inserted after migration 14 added that
    /// column. An edge from an older build (or one whose extractor never
    /// populates `import_candidates`, e.g. Rust/Go) falls straight through
    /// to pass 3.
    pub fn resolve_null_target_edges(&self, graph_version: i64) -> Result<usize> {
        let mut total_resolved = 0;

        // Pass 1: exact. The correlated subquery is evaluated against the
        // pre-update row on both sides, so `resolution_kind` is tagged only
        // for rows this pass actually binds.
        total_resolved += self.conn().execute(
            "UPDATE edges SET
                target_symbol_id = (
                    SELECT s.id FROM symbols s
                    WHERE s.qualname = edges.target_qualname
                    AND s.graph_version = edges.graph_version
                    ORDER BY s.id ASC
                    LIMIT 1
                ),
                resolution_kind = CASE WHEN (
                    SELECT s.id FROM symbols s
                    WHERE s.qualname = edges.target_qualname
                    AND s.graph_version = edges.graph_version
                    LIMIT 1
                ) IS NOT NULL THEN ? ELSE resolution_kind END
            WHERE target_symbol_id IS NULL
            AND target_qualname IS NOT NULL
            AND graph_version = ?",
            params![ResolutionKind::Exact.as_str(), graph_version],
        )?;

        const BATCH_SIZE: usize = 1000;
        let empty_symbol_map: HashMap<String, i64> = HashMap::new();

        // Pass 2: the import tier. `insert_edges` stored these as
        // unresolved + `receiver_type = ''` when the import's target
        // wasn't indexed yet, so pass 3 never touches them; this is the
        // only retry they get.
        loop {
            let mut conn = self.conn();
            let tx = conn.transaction()?;
            let batch: Vec<(i64, String, String)> = {
                let mut stmt = tx.prepare(
                    "SELECT e.id, e.import_candidates, COALESCE(f.language, 'unknown')
                     FROM edges e
                     JOIN files f ON e.file_id = f.id
                     WHERE e.target_symbol_id IS NULL
                     AND e.import_candidates IS NOT NULL
                     AND e.graph_version = ?
                     LIMIT ?",
                )?;
                let rows = stmt.query_map(params![graph_version, BATCH_SIZE], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?;
                rows.collect::<Result<Vec<_>, _>>()?
            };
            if batch.is_empty() {
                break;
            }

            let mut count = 0;
            {
                let mut resolver = Resolver::new(&tx, graph_version)?;
                let mut update_stmt = tx.prepare(
                    "UPDATE edges SET target_symbol_id = ?, resolution_kind = ? WHERE id = ?",
                )?;
                for (edge_id, candidates_json, source_lang) in &batch {
                    let candidates = decode_import_candidates(candidates_json);
                    if let Some(target_id) =
                        resolver.resolve_import(&candidates, &empty_symbol_map, source_lang)?
                    {
                        update_stmt.execute(params![
                            target_id,
                            ResolutionKind::Import.as_str(),
                            edge_id
                        ])?;
                        count += 1;
                    }
                }
            }
            tx.commit()?;
            total_resolved += count;

            // Every row in `batch` is now resolved or was tried and found
            // unresolvable; a zero-progress batch means stop, or a batch
            // full of unresolvable rows would re-select forever.
            if count == 0 {
                break;
            }
        }

        // Pass 3: the name-based tiers.
        loop {
            let mut conn = self.conn();
            let tx = conn.transaction()?;
            let unresolved: Vec<UnresolvedNameRow> = {
                let mut stmt = tx.prepare(
                    "SELECT e.id, e.target_qualname, COALESCE(f.language, 'unknown'), e.kind, e.receiver_type, f.path, e.has_receiver
                     FROM edges e
                     JOIN files f ON e.file_id = f.id
                     WHERE e.target_symbol_id IS NULL
                     AND e.target_qualname IS NOT NULL
                     AND e.graph_version = ?
                     AND (e.receiver_type IS NULL OR e.receiver_type != '')
                     LIMIT ?",
                )?;
                let rows = stmt.query_map(params![graph_version, BATCH_SIZE], |row| {
                    Ok(UnresolvedNameRow {
                        edge_id: row.get(0)?,
                        target_qualname: row.get(1)?,
                        source_lang: row.get(2)?,
                        edge_kind: row.get(3)?,
                        receiver_type: row.get(4)?,
                        file_path: row.get(5)?,
                        has_receiver: row.get(6)?,
                    })
                })?;
                rows.collect::<Result<Vec<_>, _>>()?
            };
            if unresolved.is_empty() {
                break;
            }

            let mut count = 0;
            {
                let mut resolver = Resolver::new(&tx, graph_version)?;
                let mut update_stmt = tx.prepare(
                    "UPDATE edges SET target_symbol_id = ?, resolution_kind = ? WHERE id = ?",
                )?;
                for row in &unresolved {
                    if let Some((target_id, kind)) = resolver.resolve_by_name(
                        &row.target_qualname,
                        row.receiver_type.as_deref(),
                        &row.edge_kind,
                        &row.source_lang,
                        &row.file_path,
                        !row.has_receiver,
                    )? {
                        update_stmt.execute(params![target_id, kind.as_str(), row.edge_id])?;
                        count += 1;
                    }
                }
            }
            tx.commit()?;
            total_resolved += count;

            if count == 0 {
                break;
            }
        }

        Ok(total_resolved)
    }
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
        ImportMissPolicy, LanguageProfile, Reference, Resolution, Resolver, UnresolvedReason,
        VisibilityRule, fuzzy_qualname_patterns, package_dir, primary_separator, profile_for,
        qualname_trailing_name, same_lang_patterns, two_segment_qualname_patterns,
    };
    use rusqlite::{Connection, params};

    #[test]
    fn profile_for_registers_every_visibility_rule() {
        assert_eq!(profile_for("rust").separators, &["::"]);
        assert_eq!(
            profile_for("rust").import_miss,
            ImportMissPolicy::FallThrough
        );
        assert!(profile_for("rust").normalize_import_target.is_some());
        assert_eq!(profile_for("rust").visibility, VisibilityRule::Recorded);

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
        assert!(VisibilityRule::None.is_visible(Some("private"), "a.b", "x.rs", "y.rs"));
    }

    #[test]
    fn visibility_rule_recorded_refuses_only_explicit_private() {
        let rule = VisibilityRule::Recorded;
        assert!(!rule.is_visible(Some("private"), "a.b", "x.rs", "y.rs"));
        // `NULL`/unrecorded and any other recorded value stay visible —
        // only an explicit "private" mark refuses.
        assert!(rule.is_visible(None, "a.b", "x.rs", "y.rs"));
        assert!(rule.is_visible(Some("public"), "a.b", "x.rs", "y.rs"));
    }

    #[test]
    fn visibility_rule_go_capitalization_allows_exported_anywhere() {
        let rule = VisibilityRule::GoCapitalization;
        assert!(rule.is_visible(
            None,
            "other/other.FormatGreeting",
            "other/other.go",
            "caller/caller.go"
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
            "caller/caller.go"
        ));
        // Different package — refused.
        assert!(!rule.is_visible(
            None,
            "secretpkg/secret.secretUtil",
            "secretpkg/secret.go",
            "prober/prober.go"
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
        bare_call: bool,
    ) -> Reference<'a> {
        Reference {
            target_qualname: Some(target_qualname),
            edge_kind,
            receiver_type: None,
            import_candidates: &[],
            source_lang,
            source_file_path,
            bare_call,
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
        let r = reference("caller.util", "CALLS", "python", "caller.py", true);
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
        // Python-only candidate.
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let symbol_map = std::collections::HashMap::new();
        let non_bridge = reference("caller::shared_name", "CALLS", "rust", "caller.rs", true);
        assert_eq!(
            resolver.resolve(&non_bridge, &symbol_map).unwrap(),
            Resolution::Unresolved(UnresolvedReason::NoCandidates)
        );

        // The same shape, but a Bridge Edge kind, may cross into it.
        let mut resolver = Resolver::new(&conn, 1).unwrap();
        let bridge = reference("caller::shared_name", "RPC_CALL", "rust", "caller.rs", true);
        let resolved = resolver.resolve(&bridge, &symbol_map).unwrap();
        assert!(
            matches!(resolved, Resolution::Resolved { .. }),
            "{resolved:?}"
        );
    }
}
