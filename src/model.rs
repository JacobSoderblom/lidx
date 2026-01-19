use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

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
    pub signature: Option<String>,
    pub docstring: Option<String>,
    pub graph_version: i64,
    pub commit_sha: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stable_id: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct SymbolCompact {
    pub id: i64,
    pub kind: String,
    pub name: String,
    pub qualname: String,
    pub file_path: String,
    pub start_line: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
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

#[derive(Debug, Serialize, Clone)]
pub struct Edge {
    pub id: i64,
    pub file_path: String,
    pub kind: String,
    pub source_symbol_id: Option<i64>,
    pub target_symbol_id: Option<i64>,
    pub target_qualname: Option<String>,
    pub detail: Option<String>,
    pub evidence_snippet: Option<String>,
    pub evidence_start_line: Option<i64>,
    pub evidence_end_line: Option<i64>,
    pub confidence: Option<f64>,
    pub graph_version: i64,
    pub commit_sha: Option<String>,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub event_ts: Option<i64>,
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
    pub diagnostics: DiagnosticsSummary,
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

#[derive(Debug, Serialize, Clone)]
pub struct Diagnostic {
    pub id: i64,
    pub path: Option<String>,
    pub line: Option<i64>,
    pub column: Option<i64>,
    pub end_line: Option<i64>,
    pub end_column: Option<i64>,
    pub severity: Option<String>,
    pub message: String,
    pub rule_id: Option<String>,
    pub tool: Option<String>,
    pub snippet: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DiagnosticsSummary {
    pub total: i64,
    pub by_severity: BTreeMap<String, i64>,
    pub by_tool: BTreeMap<String, i64>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct SearchHit {
    pub path: String,
    pub line: usize,
    pub column: usize,
    pub line_text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Vec<ContextLine>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enclosing_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasons: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_hops: Option<Vec<RpcSuggestion>>,
}

#[derive(Debug, Serialize, Clone)]
pub struct GrepHit {
    pub path: String,
    pub line: usize,
    pub column: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Vec<ContextLine>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enclosing_symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasons: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
pub struct FlowStatusEntry {
    pub path: String,
    pub route_count: usize,
    pub call_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routes: Option<Vec<Edge>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calls: Option<Vec<Edge>>,
}

#[derive(Debug, Serialize)]
pub struct FlowStatusResult {
    pub routes_total: usize,
    pub calls_total: usize,
    pub edge_limit: usize,
    pub truncated: bool,
    pub routes_without_calls: Vec<FlowStatusEntry>,
    pub calls_without_routes: Vec<FlowStatusEntry>,
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
    /// Estimated total bytes (populated in dry_run mode)
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callees: Option<Vec<ExplainRef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tests: Option<Vec<ExplainRef>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub implements: Option<Vec<Symbol>>,
    pub budget: BudgetInfo,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub next_hops: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ExplainRef {
    pub symbol: Symbol,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub evidence: Option<String>,
    pub edge_kind: String,
}

#[derive(Debug, Serialize)]
pub struct BudgetInfo {
    pub budget_bytes: usize,
    pub used_bytes: usize,
    pub truncated: bool,
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
    pub downstream: Vec<DiffImpactEntry>,
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
    pub change_type: String,  // "modified", "signature_changed", "added", "deleted"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_signature: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DiffImpactEntry {
    pub symbol: Symbol,
    pub relationship: String,  // "calls", "imports", "extends"
    pub distance: usize,
    pub confidence: f64,
}

#[derive(Debug, Serialize)]
pub struct TestCoverageEntry {
    pub symbol_qualname: String,
    pub tests: Vec<TestRef>,
    pub status: String,  // "covered", "uncovered"
}

#[derive(Debug, Serialize)]
pub struct TestRef {
    pub test_qualname: String,
    pub test_file: String,
    pub coverage_type: String,  // "direct", "indirect"
}

#[derive(Debug, Serialize)]
pub struct RiskAssessment {
    pub level: String,  // "low", "medium", "high", "critical"
    pub factors: Vec<RiskFactor>,
    pub focus_areas: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub review_checklist: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RiskFactor {
    pub factor: String,
    pub description: String,
    pub severity: String,  // "low", "medium", "high"
}

// trace_flow types

#[derive(Debug, Serialize)]
pub struct TraceFlowResult {
    pub start: Symbol,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<Symbol>,
    pub trace: Vec<TraceHop>,
    pub paths_found: usize,
    pub reached_target: bool,
    pub truncated: bool,
    pub budget: BudgetInfo,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub next_hops: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct TraceHop {
    pub symbol: Symbol,
    pub edge_kind: String,
    pub distance: usize,
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
}
