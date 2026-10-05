use serde::Serialize;
use serde_json::Value;

/// Longest struct/enum signature shown in a response. The stored signature
/// stays whole (deferred Rust receivers read field types from it).
const MAX_DISPLAY_SIGNATURE: usize = 240;

/// A struct/enum signature capped at `MAX_DISPLAY_SIGNATURE` bytes on a
/// field/variant boundary, ending `, …`; anything else is returned as is.
pub fn display_signature(signature: &str) -> std::borrow::Cow<'_, str> {
    let is_adt = signature.starts_with("struct ") || signature.starts_with("enum ");
    if !is_adt || signature.len() <= MAX_DISPLAY_SIGNATURE {
        return signature.into();
    }
    let mut end = MAX_DISPLAY_SIGNATURE;
    while !signature.is_char_boundary(end) {
        end -= 1;
    }
    let head = &signature[..end];
    let cut = head.rfind(", ").unwrap_or(head.len());
    format!("{}, …", &head[..cut]).into()
}

/// Prefix a C# `partial` type declaration's stored `signature` carries
/// (`partial`, or `partial (int X)` with a primary constructor). It is
/// resolver-only metadata (issue #206): `public_signature` strips it from
/// every symbol read back out of the store.
pub const PARTIAL_SIGNATURE_MARKER: &str = "partial";

/// Stored signature for a C# type: `params` (a primary-constructor parameter
/// list) with the partial marker added when `partial`.
pub fn type_signature_with_partial(params: Option<String>, partial: bool) -> Option<String> {
    match (params, partial) {
        (Some(p), true) => Some(format!("{PARTIAL_SIGNATURE_MARKER} {p}")),
        (None, true) => Some(PARTIAL_SIGNATURE_MARKER.to_string()),
        (p, false) => p,
    }
}

/// Whether a stored signature marks a `partial` type declaration.
pub fn is_partial_signature(signature: Option<&str>) -> bool {
    signature.is_some_and(|s| {
        s == PARTIAL_SIGNATURE_MARKER
            || s.strip_prefix(PARTIAL_SIGNATURE_MARKER)
                .is_some_and(|rest| rest.starts_with(' '))
    })
}

/// A type signature without the partial marker: `None` when nothing but the
/// marker was stored.
pub fn public_signature(signature: Option<String>) -> Option<String> {
    let sig = signature?;
    if !is_partial_signature(Some(&sig)) {
        return Some(sig);
    }
    let rest = sig[PARTIAL_SIGNATURE_MARKER.len()..].trim();
    (!rest.is_empty()).then(|| rest.to_string())
}

/// Whether a stored type signature carries a primary-constructor parameter
/// list (`record R(int A)`), partial-marked or not.
pub fn has_parameter_list(signature: Option<&str>) -> bool {
    signature.is_some_and(|s| {
        s.strip_prefix(PARTIAL_SIGNATURE_MARKER)
            .map_or(s, str::trim_start)
            .starts_with('(')
    })
}

fn serialize_signature<S: serde::Serializer>(
    signature: &Option<String>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match signature {
        Some(sig) => serializer.serialize_some(display_signature(sig).as_ref()),
        None => serializer.serialize_none(),
    }
}

fn is_false(value: &bool) -> bool {
    !value
}

#[derive(Debug, Serialize, Clone)]
pub struct Symbol {
    pub id: i64,
    pub file_path: String,
    pub kind: String,
    pub name: String,
    pub qualname: String,
    pub start_line: i64,
    pub start_col: i64,
    pub end_line: i64,
    pub end_col: i64,
    pub start_byte: i64,
    pub end_byte: i64,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_signature"
    )]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub docstring: Option<String>,
    pub graph_version: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_id: Option<String>,
}

impl Symbol {
    /// True for an external stub: a symbol attributed to a synthetic `ext:`
    /// location rather than a real repo file (e.g. a known third-party
    /// import target), which every repo-internal listing excludes.
    pub fn is_external(&self) -> bool {
        self.kind == "external" || self.qualname.starts_with("ext:")
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct SymbolCompact {
    pub id: i64,
    pub kind: String,
    pub name: String,
    pub qualname: String,
    pub file_path: String,
    pub start_line: i64,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_signature"
    )]
    pub signature: Option<String>,
}

impl From<Symbol> for SymbolCompact {
    fn from(s: Symbol) -> Self {
        SymbolCompact {
            id: s.id,
            kind: s.kind,
            name: s.name,
            qualname: s.qualname,
            file_path: s.file_path,
            start_line: s.start_line,
            signature: s.signature,
        }
    }
}

impl From<&Symbol> for SymbolCompact {
    fn from(s: &Symbol) -> Self {
        SymbolCompact {
            id: s.id,
            kind: s.kind.clone(),
            name: s.name.clone(),
            qualname: s.qualname.clone(),
            file_path: s.file_path.clone(),
            start_line: s.start_line,
            signature: s.signature.clone(),
        }
    }
}

/// One entry in an `outline` response: a symbol (or, for Markdown, a heading)
/// in source order, with no body. `parent` is the qualname of the nearest
/// containing entry within the same file (omitted for top-level entries).
#[derive(Debug, Serialize, Clone)]
pub struct OutlineEntry {
    pub kind: String,
    pub name: String,
    pub qualname: String,
    #[serde(
        skip_serializing_if = "Option::is_none",
        serialize_with = "serialize_signature"
    )]
    pub signature: Option<String>,
    pub start_line: i64,
    pub end_line: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// First line of the symbol's docstring, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

impl OutlineEntry {
    /// Builds an outline entry from an indexed `Symbol`'s own fields, under
    /// `parent`'s qualname (`None` for a top-level entry) with a
    /// caller-computed `doc` (each caller derives it from `docstring`
    /// differently: a fresh borrow vs. an already-owned `Symbol`) -- the field
    /// mapping shared by every symbol-derived outline/skeleton-children entry.
    /// A Markdown heading entry has no backing `Symbol` and builds its own
    /// literal instead.
    pub fn from_symbol(symbol: &Symbol, parent: Option<String>, doc: Option<String>) -> Self {
        OutlineEntry {
            kind: symbol.kind.clone(),
            name: symbol.name.clone(),
            qualname: symbol.qualname.clone(),
            signature: symbol.signature.clone(),
            start_line: symbol.start_line,
            end_line: symbol.end_line,
            parent,
            doc,
        }
    }
}

/// Response for `outline`: a compact, no-bodies skeleton of an indexed file.
#[derive(Debug, Serialize, Clone)]
pub struct OutlineResult {
    pub path: String,
    pub language: String,
    pub total_lines: i64,
    pub entries: Vec<OutlineEntry>,
    /// True when the file changed on disk since indexing, so `entries` line
    /// spans may be out of date. Omitted when false.
    #[serde(skip_serializing_if = "is_false")]
    pub stale: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub next_hops: Vec<Value>,
}

/// Header and payload shared by `read_symbol`'s three response shapes: a full
/// source read, a container's `skeleton` (children only, no bodies), and an
/// over-budget stub (`omitted: true`). Every shape shares the header fields
/// (`qualname`/`kind`/`path`/`start_line`/`end_line`/`stale`); each fills in
/// only the payload fields it uses, and the rest are skipped from the JSON
/// (`skip_serializing_if`) rather than emitted as `null`, so the field set
/// for a given shape matches what it always has.
#[derive(Debug, Serialize, Clone)]
pub struct ReadSymbolEntry {
    pub qualname: String,
    pub kind: String,
    pub path: String,
    pub start_line: i64,
    pub end_line: i64,
    pub stale: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skeleton: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<OutlineEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub omitted: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub next_hops: Vec<Value>,
}

impl ReadSymbolEntry {
    /// Header fields shared by every `read_symbol` response shape, with every
    /// payload field defaulted -- each of the three constructors below fills
    /// in only the payload fields its shape uses.
    fn header(symbol: &Symbol, stale: bool) -> Self {
        ReadSymbolEntry {
            qualname: symbol.qualname.clone(),
            kind: symbol.kind.clone(),
            path: symbol.file_path.clone(),
            start_line: symbol.start_line,
            end_line: symbol.end_line,
            stale,
            source: None,
            skeleton: None,
            children: None,
            omitted: None,
            size_bytes: None,
            next_hops: Vec::new(),
        }
    }

    /// A full source read: `source` filled in, no skeleton/omitted payload.
    pub fn source(symbol: &Symbol, stale: bool, source: String) -> Self {
        ReadSymbolEntry {
            source: Some(source),
            ..Self::header(symbol, stale)
        }
    }

    /// A container's skeleton: `children`'s signatures/line ranges, no body.
    pub fn skeleton(symbol: &Symbol, stale: bool, children: Vec<OutlineEntry>) -> Self {
        ReadSymbolEntry {
            skeleton: Some(true),
            children: Some(children),
            ..Self::header(symbol, stale)
        }
    }

    /// An over-budget stub: header fields only, plus `omitted: true` and the
    /// actual (over-budget) size -- never a partial/cut source.
    pub fn omitted_header(symbol: &Symbol, stale: bool, size_bytes: usize) -> Self {
        ReadSymbolEntry {
            omitted: Some(true),
            size_bytes: Some(size_bytes),
            ..Self::header(symbol, stale)
        }
    }
}

/// `resolution_kind` of the synthetic CALLS edges built by
/// `Db::edges_for_symbols_with_dispatch` (interface method -> implementor).
pub const INTERFACE_DISPATCH_KIND: &str = "interface_dispatch";

#[derive(Debug, Serialize, Clone)]
pub struct Edge {
    pub id: i64,
    pub file_path: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_symbol_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_symbol_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_qualname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence_snippet: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence_start_line: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence_end_line: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f64>,
    /// The tier that bound `target_symbol_id` (`exact`, `import`,
    /// `receiver_type`, `inherited`, `two_segment`, `bare_name`, or
    /// `external` -- see `db::resolver::ResolutionKind::as_str`), or
    /// absent when the target was never bound at all: a still-pending
    /// String-Targeted Edge Kind (a Bridge Edge kind's cross-process join
    /// key, or a CONFIG_SOURCE/CONFIG_READ/CONFIG_BIND kind's config
    /// key/secret URI), a structural edge kind the resolver doesn't
    /// label, or a graph indexed before this field existed. Distinct from
    /// `confidence` (extraction certainty) and, on `analyze_impact`, from
    /// `min_confidence` (a query-time impact heuristic) -- neither of
    /// those describes how the target was found.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_kind: Option<String>,
    pub graph_version: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_id: Option<String>,
    pub event_ts: Option<i64>,
    /// Synthetic interface-dispatch edges to a closed generic explicit impl
    /// (`C.IA<int>.Run`) carry its type arguments: only a call whose
    /// receiver has the same ones reaches it.
    #[serde(skip)]
    pub dispatch_args: Option<String>,
}

impl Edge {
    /// Whether this edge was synthesized by interface dispatch rather than
    /// stored in the index (it has no row, so `id` is 0).
    pub fn is_synthetic(&self) -> bool {
        self.id == 0 && self.resolution_kind.as_deref() == Some(INTERFACE_DISPATCH_KIND)
    }
}

/// One edge, normalized for the golden-corpus correctness scoreboard
/// (`Db::edges_snapshot`): the source and (if resolved) target's actual
/// qualnames, rather than the edge's raw stored `target_qualname` text
/// (which is the call site's literal, pre-resolution guess — see
/// `resolve_call_target` — and often differs from the resolved symbol's
/// real qualname).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct EdgeSnapshotRow {
    pub source_qualname: String,
    pub kind: String,
    /// `None` when `target_symbol_id` is NULL (unresolved).
    pub target_qualname: Option<String>,
    pub resolution_kind: Option<String>,
}

/// One `(language, reason, count)` bucket of `Db::unresolved_reference_summary`
/// (issue #78): how many rows the `unresolved_references` store currently
/// holds for that language and `UnresolvedReason`, at the queried graph
/// version. Test/reporting support for the golden-corpus scoreboard, same
/// spirit as `EdgeSnapshotRow`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct UnresolvedReferenceSummary {
    pub language: String,
    pub reason: String,
    pub count: i64,
}

#[derive(Debug, Serialize)]
pub struct RepoOverview {
    pub repo_root: String,
    pub files: i64,
    pub symbols: i64,
    pub edges: i64,
    pub last_indexed: Option<i64>,
    pub graph_version: Option<i64>,
    pub commit_sha: Option<String>,
    pub scope_counts: ScopeCounts,
}

/// Per-scope file counts (issue #63), aggregated from the same query-time
/// classifier `search::scope_allows` uses for the `search` method's `scope`
/// param -- not a stored column. A file can satisfy more than one scope
/// (e.g. `docs/build.py` is both `docs` and, unlike `code`, not mutually
/// exclusive with it), so these counts are not guaranteed to sum to
/// `RepoOverview::files`. Every field is always present, including zero --
/// this is what makes an empty `tests` list elsewhere in a response
/// interpretable rather than ambiguous.
#[derive(Debug, Serialize, Clone, Copy, Default)]
pub struct ScopeCounts {
    pub code: i64,
    pub tests: i64,
    pub docs: i64,
    pub examples: i64,
}

#[derive(Debug, Serialize)]
pub struct RepoInsights {
    pub repo_root: String,
    pub call_edges: i64,
    pub top_complexity: Vec<SymbolComplexity>,
    pub duplicate_groups: Vec<DuplicateGroup>,
    pub top_fan_in: Vec<SymbolCoupling>,
    pub top_fan_out: Vec<SymbolCoupling>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coupling_hotspots: Option<Vec<CouplingHotspot>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staleness: Option<StalenessMetrics>,
    pub last_indexed: Option<i64>,
    pub graph_version: Option<i64>,
    pub commit_sha: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct StalenessMetrics {
    pub dead_symbols: i64,
    pub unused_imports: i64,
    pub orphan_tests: i64,
}

#[derive(Debug, Serialize, Clone)]
pub struct FileMetrics {
    pub path: String,
    pub loc: i64,
    pub blank: i64,
    pub comment: i64,
    pub code: i64,
}

#[derive(Debug, Serialize, Clone)]
pub struct SymbolComplexity {
    pub symbol: Symbol,
    pub loc: i64,
    pub complexity: i64,
}

#[derive(Debug, Serialize, Clone)]
pub struct SymbolCoupling {
    pub symbol: Symbol,
    pub count: i64,
}

#[derive(Debug, Serialize, Clone)]
pub struct SymbolMetrics {
    pub symbol: Symbol,
    pub loc: i64,
    pub complexity: i64,
    pub duplication_hash: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DuplicateGroup {
    pub hash: String,
    pub count: i64,
    pub symbols: Vec<Symbol>,
}

#[derive(Debug, Serialize)]
pub struct OpenSymbolResult {
    pub symbol: Symbol,
    pub snippet: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct ContextLine {
    pub line: usize,
    pub text: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct RpcSuggestion {
    pub method: String,
    pub params: Value,
    // Named `description` (not `label`) to match every other handler's
    // hand-rolled `next_hops` entries (see e.g. explain_symbol in
    // `src/rpc/handlers.rs`, outline/read_symbol in `src/rpc/reading.rs`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct SearchHit {
    pub path: String,
    pub line: usize,
    #[serde(skip)]
    pub column: usize,
    pub line_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Vec<ContextLine>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enclosing_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    #[serde(skip)]
    pub reasons: Option<Vec<String>>,
    #[serde(skip)]
    pub engine: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_hops: Option<Vec<RpcSuggestion>>,
}

#[derive(Debug, Serialize, Clone)]
pub struct GrepHit {
    pub path: String,
    pub line: usize,
    #[serde(skip)]
    pub column: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Vec<ContextLine>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enclosing_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    #[serde(skip)]
    pub reasons: Option<Vec<String>>,
    #[serde(skip)]
    pub engine: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_hops: Option<Vec<RpcSuggestion>>,
}

#[derive(Debug, Serialize)]
pub struct ChangedFilesResult {
    pub added: Vec<String>,
    pub modified: Vec<String>,
    pub deleted: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct IndexChangeCounts {
    pub added: usize,
    pub modified: usize,
    pub deleted: usize,
}

#[derive(Debug, Serialize)]
pub struct IndexStatus {
    pub repo_root: String,
    pub last_indexed: Option<i64>,
    pub graph_version: Option<i64>,
    pub commit_sha: Option<String>,
    pub stale: bool,
    pub hint: String,
    pub counts: IndexChangeCounts,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changed_files: Option<ChangedFilesResult>,
}

#[derive(Debug, Serialize)]
pub struct Subgraph {
    pub nodes: Vec<Symbol>,
    pub edges: Vec<Edge>,
}

#[derive(Debug, Serialize, Clone)]
pub struct GraphVersion {
    pub id: i64,
    pub created: i64,
    pub commit_sha: Option<String>,
}

/// Is this XREF edge trustworthy enough to traverse?
///
/// XREF is name matching over string literals, and it comes in two grades that
/// the extractor already distinguishes:
///
/// - `qualname_exact` (confidence 1.0) -- a *qualified* token matched, e.g. the
///   literal `"dpb.catalog_publication"` inside a SQL query naming that exact
///   table. Across dpb all 61 of these land on real schema or proto objects.
/// - `name_exact` (confidence 0.7-0.8) -- a single bare word matched, e.g. the
///   token `Deserialize` matching a Rust `use serde::Deserialize`, a Python
///   docstring and an unrelated C# method. All 550 of these are noise, and they
///   fabricated a 16-step multi-language blast radius for one C# method.
///
/// Only the qualified grade may drive an answer. The bare grade stays in the
/// database for anyone querying it directly, but no traversal crosses it.
// ponytail: keyed off the `match` field the extractor already writes, rather
// than a confidence threshold -- confidences get retuned, the grade names do
// not. If a third grade appears, this becomes a match on an enum.
pub fn xref_is_traversable(edge: &Edge) -> bool {
    if edge.kind != "XREF" {
        return true;
    }
    edge.detail
        .as_deref()
        .and_then(|d| serde_json::from_str::<serde_json::Value>(d).ok())
        .and_then(|v| {
            v.get("match")
                .and_then(|m| m.as_str())
                .map(|m| m == "qualname_exact")
        })
        .unwrap_or(false)
}

/// Whether an edge's own resolution kind is one of `exclude` -- the shared
/// `exclude_resolution_kinds` predicate (issue #81) behind `trace_flow`
/// (`traversal.rs`), the direct impact layer (`impact/layers/direct.rs`),
/// and the test impact layer (`impact/layers/test.rs`). `resolution_kind`
/// is `Edge::resolution_kind` (`edge.resolution_kind.as_deref()`); `None`
/// means the edge has no resolution kind at all -- a String-Targeted Edge
/// Kind (Bridge Edge or CONFIG_*) or any edge kind the resolver never
/// labels -- and is never excluded by this check.
pub fn is_resolution_excluded(resolution_kind: Option<&str>, exclude: &[String]) -> bool {
    resolution_kind.is_some_and(|rk| exclude.iter().any(|k| k == rk))
}

#[derive(Debug, Serialize)]
pub struct EdgeReference {
    pub edge: Edge,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<Symbol>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<Symbol>,
}

#[derive(Debug, Serialize)]
pub struct ReferencesResult {
    pub symbol: Symbol,
    pub incoming: Vec<EdgeReference>,
    pub outgoing: Vec<EdgeReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<ReferencesMetadata>,
}

#[derive(Debug, Serialize)]
pub struct ReferencesMetadata {
    pub aggregated_members: usize,
    pub note: String,
}

#[derive(Debug, Serialize)]
pub struct RouteRefsResult {
    pub query: String,
    pub normalized: String,
    pub references: Vec<EdgeReference>,
}

#[derive(Debug, Serialize)]
pub struct IndexStats {
    pub scanned: usize,
    pub indexed: usize,
    pub skipped: usize,
    pub deleted: usize,
    pub symbols: usize,
    pub edges: usize,
    pub duration_ms: u64,
    /// Post-reindex graph-version prune failure; the reindex itself still succeeded.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prune_error: Option<String>,
}

// gather_context types

/// Type of source for a context item
#[derive(Debug, Serialize, Clone)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    DirectSeed,
    Subgraph,
    Search,
}

/// Structured source information for context items
#[derive(Debug, Serialize, Clone)]
pub struct ItemSource {
    /// Type of source
    pub source_type: SourceType,
    /// Index of originating seed (if applicable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed_index: Option<usize>,
    /// Relationship to seed symbol (calls, called_by, contains, etc.)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relationship: Option<String>,
    /// Graph distance from seed (0 = seed itself)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance: Option<usize>,
}

/// Location of a search match within a file
#[derive(Debug, Serialize, Clone)]
pub struct MatchLocation {
    /// Line number of match (1-indexed)
    pub line: i64,
    /// Column of match start (1-indexed)
    pub column: i64,
    /// The matched text
    pub match_text: String,
}

#[derive(Debug, Serialize)]
pub struct GatherContextResult {
    /// Ordered list of context items
    pub items: Vec<ContextItem>,
    /// Total bytes of content returned
    pub total_bytes: usize,
    /// Byte budget that was used
    pub budget_bytes: usize,
    /// Whether budget was exhausted before all seeds processed
    pub truncated: bool,
    /// Estimated total bytes (populated in dry_run mode): the total a real run would return
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_bytes: Option<usize>,
    /// Processing metadata
    pub metadata: ContextMetadata,
}

#[derive(Debug, Serialize, Clone)]
pub struct ContextItem {
    /// Structured source information
    pub source: ItemSource,
    /// File path
    pub path: String,
    /// Line range (if applicable)
    pub start_line: Option<i64>,
    pub end_line: Option<i64>,
    /// Byte range in file
    pub start_byte: i64,
    pub end_byte: i64,
    /// The actual content
    pub content: String,
    /// Associated symbol (if from symbol seed or subgraph)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<Symbol>,
    /// Relevance score (for search results)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    /// Location of search match within content (for search seeds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_location: Option<MatchLocation>,
}

#[derive(Debug, Serialize)]
pub struct ContextMetadata {
    /// Number of seeds processed successfully
    pub seeds_processed: usize,
    /// Number of seeds skipped
    pub seeds_skipped: usize,
    /// Detailed reasons for skipped seeds
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub skip_reasons: Vec<SkipReason>,
    /// Number of symbols resolved
    pub symbols_resolved: usize,
    /// Number of items deduplicated
    pub items_deduplicated: usize,
    /// Processing time in milliseconds
    pub duration_ms: u64,
}

/// Reason why a seed was skipped during gather_context
#[derive(Debug, Serialize, Clone)]
pub struct SkipReason {
    /// Index of the seed in the input array
    pub seed_index: usize,
    /// Type of seed: "symbol", "file", or "search"
    pub seed_type: String,
    /// The seed value (qualname, path, or query)
    pub seed_value: String,
    /// Machine-readable error code
    pub code: String,
    /// Human-readable explanation
    pub message: String,
    /// Suggested alternatives (for typos)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub suggestions: Vec<String>,
}

impl SkipReason {
    pub fn symbol_not_found(index: usize, qualname: &str, suggestions: Vec<String>) -> Self {
        let message = if suggestions.is_empty() {
            format!("Symbol not found: '{}'", qualname)
        } else {
            format!(
                "Symbol not found: '{}'. Did you mean: {}?",
                qualname,
                suggestions.join(", ")
            )
        };
        Self {
            seed_index: index,
            seed_type: "symbol".to_string(),
            seed_value: qualname.to_string(),
            code: "symbol_not_found".to_string(),
            message,
            suggestions,
        }
    }

    pub fn file_not_found(index: usize, path: &str) -> Self {
        Self {
            seed_index: index,
            seed_type: "file".to_string(),
            seed_value: path.to_string(),
            code: "file_not_found".to_string(),
            message: format!("File not found: '{}'", path),
            suggestions: vec![],
        }
    }

    pub fn file_outside_repo(index: usize, path: &str) -> Self {
        Self {
            seed_index: index,
            seed_type: "file".to_string(),
            seed_value: path.to_string(),
            code: "file_outside_repo".to_string(),
            message: format!("Path '{}' is outside repository root", path),
            suggestions: vec![],
        }
    }

    pub fn search_no_results(index: usize, query: &str) -> Self {
        Self {
            seed_index: index,
            seed_type: "search".to_string(),
            seed_value: query.to_string(),
            code: "search_no_results".to_string(),
            message: format!("Search query '{}' returned no results", query),
            suggestions: vec![],
        }
    }

    pub fn invalid_line_range(index: usize, path: &str, start: i64, end: i64) -> Self {
        Self {
            seed_index: index,
            seed_type: "file".to_string(),
            seed_value: path.to_string(),
            code: "invalid_line_range".to_string(),
            message: format!("Invalid line range {}-{} for file '{}'", start, end, path),
            suggestions: vec![],
        }
    }
}

/// Validation error for parameter checking
#[derive(Debug, Serialize)]
pub struct ValidationError {
    pub field: String,
    pub code: String,
    pub message: String,
}

/// Collection of validation errors
#[derive(Debug, Serialize)]
pub struct ValidationResult {
    pub errors: Vec<ValidationError>,
}

impl Default for ValidationResult {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidationResult {
    pub fn new() -> Self {
        Self { errors: Vec::new() }
    }

    pub fn add(&mut self, field: &str, code: &str, message: &str) {
        self.errors.push(ValidationError {
            field: field.to_string(),
            code: code.to_string(),
            message: message.to_string(),
        });
    }

    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }
}

// Impact analysis types (re-exported from impact module for backward compatibility)
pub use crate::impact::types::{
    FileImpact, ImpactConfig, ImpactEntry, ImpactPath, ImpactResult, ImpactSummary, PathStep,
};

// Co-change types

#[derive(Debug, Serialize, Clone)]
pub struct CoChangeResult {
    pub file_a: String,
    pub file_b: String,
    pub co_change_count: i64,
    pub confidence: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_commit_sha: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct CouplingHotspot {
    pub file_a: String,
    pub file_b: String,
    pub confidence: f64,
    pub co_change_count: i64,
}

// explain_symbol types

#[derive(Debug, Serialize)]
pub struct ExplainSymbolResult {
    pub symbol: Symbol,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callers: Option<Vec<ExplainRef>>,
    /// True count of matching callers found, before `max_refs`/byte-budget
    /// capping. Present whenever `callers` is present, so a caller can always
    /// tell `callers.len() < callers_total` apart from "there just aren't more".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callers_total: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callees: Option<Vec<ExplainRef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callees_total: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tests: Option<Vec<ExplainRef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tests_total: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub implements: Option<Vec<Symbol>>,
    /// True count of matching supertypes/interfaces found, before
    /// `max_refs`/byte-budget capping. Present whenever `implements` is
    /// present, so a caller can always tell `implements.len() <
    /// implements_total` apart from "there just aren't more".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub implements_total: Option<usize>,
    /// `commit_sha`/`graph_version` are properties of the indexing run, not
    /// of any one symbol, so they're stamped here once for the whole
    /// response rather than on `symbol` and every `ExplainRef` (issue #66).
    pub graph_version: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub commit_sha: Option<String>,
    pub budget: BudgetInfo,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub next_hops: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExplainRef {
    pub symbol: Symbol,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<String>,
    pub edge_kind: String,
    /// Service/rpc, route, channel or config key for a cross-boundary
    /// (RPC_CALL, HTTP_CALL, CHANNEL_PUBLISH, CONFIG_READ, ...) ref.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_context: Option<serde_json::Value>,
    /// The tier that bound this ref's edge -- see `Edge::resolution_kind`.
    /// Absent when the edge itself never carries one (a still-pending
    /// String-Targeted Edge Kind -- Bridge Edge or CONFIG_* -- or an edge
    /// kind the resolver doesn't label).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_kind: Option<String>,
    /// The ref reaches the symbol only through interface dispatch (a call
    /// to the interface method, not to this implementation).
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub via_interface: bool,
    /// A JS/TS test attributed to the whole test file (its module symbol)
    /// because the calling `describe`/`it`/`test` callback is anonymous: it
    /// says which file covers the symbol, not which test.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub file_level: bool,
}

#[derive(Debug, Serialize)]
pub struct BudgetInfo {
    pub budget_bytes: usize,
    pub used_bytes: usize,
    pub truncated: bool,
    /// The `max_bytes` the caller actually requested, when it differs from
    /// `budget_bytes` because the request was silently clamped to a hard cap.
    /// `None` when no clamping happened (including when the caller didn't
    /// pass `max_bytes` at all).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_bytes: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct ModuleMapResult {
    pub modules: Vec<ModuleNode>,
    pub edges: Vec<ModuleEdge>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub next_hops: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ModuleNode {
    pub path: String,
    pub symbol_count: usize,
    pub file_count: usize,
    pub languages: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ModuleEdge {
    pub source_module: String,
    pub target_module: String,
    pub call_count: usize,
    pub import_count: usize,
    pub xref_count: usize,
}

// find_tests_for types

#[derive(Debug, Serialize)]
pub struct FindTestsResult {
    pub symbol: SymbolCompact,
    pub direct_tests: Vec<TestMatch>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub indirect_tests: Vec<TestMatch>,
    pub summary: TestSummary,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub next_hops: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct TestMatch {
    pub test_symbol: SymbolCompact,
    pub match_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub via_symbol: Option<SymbolCompact>,
    pub relevance: f64,
}

#[derive(Debug, Serialize)]
pub struct TestSummary {
    pub direct_count: usize,
    pub indirect_count: usize,
    pub test_files: Vec<String>,
}

// analyze_diff types

#[derive(Debug, Serialize)]
pub struct AnalyzeDiffResult {
    pub changed_symbols: Vec<ChangedSymbol>,
    // Callers of the changed symbols found via BFS -- i.e. what depends on
    // the change, which makes this upstream, not downstream.
    pub upstream: Vec<DiffImpactEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_coverage: Option<Vec<TestCoverageEntry>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub risk: Option<RiskAssessment>,
    pub budget: BudgetInfo,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub next_hops: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ChangedSymbol {
    pub symbol: Symbol,
    pub change_type: String, // "modified", "signature_changed", "added", "deleted", "in_changed_file"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_signature: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DiffImpactEntry {
    pub symbol: Symbol,
    pub relationship: String, // "caller", "caller_depth_2", "caller_depth_3", ...
    pub distance: usize,
    pub confidence: f64,
    /// The tier that bound the edge connecting this entry to the previous
    /// BFS level -- see `Edge::resolution_kind`. Absent when that edge
    /// never carries one (a still-pending String-Targeted Edge Kind --
    /// Bridge Edge or CONFIG_* -- or an edge kind the resolver doesn't
    /// label).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_kind: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct TestCoverageEntry {
    pub symbol_qualname: String,
    pub tests: Vec<TestRef>,
    pub status: String, // "covered", "covered_via_interface", "uncovered"
}

#[derive(Debug, Serialize)]
pub struct TestRef {
    pub test_qualname: String,
    pub test_file: String,
    pub coverage_type: String, // "direct", "via_interface"
}

#[derive(Debug, Serialize)]
pub struct RiskAssessment {
    pub level: String, // "low", "medium", "high", "critical"
    pub factors: Vec<RiskFactor>,
    pub focus_areas: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub review_checklist: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RiskFactor {
    pub factor: String,
    pub description: String,
    pub severity: String, // "low", "medium", "high"
}

/// Lower-bound indicator (issue #81): attached to `trace_flow`/
/// `analyze_impact` results when pending `unresolved_references` rows
/// touch the traversed symbols, so an agent doesn't mistake a partial
/// answer for a complete one. See
/// `Db::unresolved_reference_count_for_symbols`'s doc for exactly what
/// counts as "touching" a traversed symbol, per direction.
#[derive(Debug, Serialize, Clone, Copy)]
pub struct LowerBound {
    pub is_lower_bound: bool,
    pub unresolved_count: i64,
}

// trace_flow types

#[derive(Debug, Serialize)]
pub struct TraceFlowResult {
    pub start: Symbol,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<Symbol>,
    pub trace: Vec<TraceHop>,
    /// Without an end target: number of leaf hops (hops nothing else was
    /// reached through, including hops at the `max_hops` ceiling); the trace
    /// is node-deduplicated, so this is not a distinct-path count.
    /// Non-decreasing in `max_hops`. With an end target: 1 if reached, else 0.
    pub paths_found: usize,
    /// Total settled trace nodes, independent of `trace_offset` and byte
    /// truncation.
    pub nodes_found: usize,
    pub reached_target: bool,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation_reason: Option<String>,
    pub budget: BudgetInfo,
    pub lower_bound: LowerBound,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub next_hops: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct TraceHop {
    pub symbol: Symbol,
    pub edge_kind: String,
    pub distance: usize,
    /// Qualname of the node this hop was reached from (a seed for the
    /// first hop), so a caller can rebuild the chain by following it back.
    pub predecessor: String,
    /// Symbol id of that node, for exact (qualname-collision-proof) backtracking.
    #[serde(skip)]
    pub predecessor_id: i64,
    pub language: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snippet: Option<String>,
    pub cross_language: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boundary_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub boundary_detail: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_context: Option<serde_json::Value>,
    /// The tier that bound the edge this hop traversed -- see
    /// `Edge::resolution_kind`. Absent when the edge itself never carries
    /// one (a still-pending String-Targeted Edge Kind -- Bridge Edge or
    /// CONFIG_* -- or an edge kind the resolver doesn't label).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_kind: Option<String>,
    /// Which way a bridged hop was crossed: upstream (to a caller or
    /// publisher) or downstream (to a callee or subscriber). Absent on a
    /// hop reached over a direct edge.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bridge_direction: Option<crate::indexer::channel::WalkDirection>,
}

#[cfg(test)]
mod display_signature_tests {
    use super::display_signature;

    #[test]
    fn long_struct_signature_is_capped_on_a_field_boundary() {
        let fields: Vec<String> = (0..60).map(|i| format!("field_{i}: Type{i}")).collect();
        let full = format!("struct Big {{ {} }}", fields.join(", "));
        let shown = display_signature(&full);
        assert!(shown.len() < full.len());
        assert!(shown.starts_with("struct Big { field_0: Type0, field_1"));
        assert!(shown.ends_with(", \u{2026}"));
        assert!(!shown.contains("field_59"));
    }

    #[test]
    fn responses_serialize_the_capped_signature() {
        let fields: Vec<String> = (0..60).map(|i| format!("field_{i}: Type{i}")).collect();
        let full = format!("struct Big {{ {} }}", fields.join(", "));
        let compact = super::SymbolCompact {
            id: 1,
            kind: "struct".into(),
            name: "Big".into(),
            qualname: "crate::Big".into(),
            file_path: "a.rs".into(),
            start_line: 1,
            signature: Some(full.clone()),
        };
        let json = serde_json::to_value(&compact).unwrap();
        let shown = json["signature"].as_str().unwrap();
        assert!(shown.len() < full.len() && shown.ends_with('\u{2026}'));
        // The value itself is untouched.
        assert_eq!(compact.signature.as_deref(), Some(full.as_str()));
    }

    #[test]
    fn short_and_non_adt_signatures_are_untouched() {
        assert_eq!(
            display_signature("struct P { x: u8 }"),
            "struct P { x: u8 }"
        );
        let long_fn = format!("({})", "a: u8, ".repeat(100));
        assert_eq!(display_signature(&long_fn), long_fn.as_str());
    }
}
