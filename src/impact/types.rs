//! Type definitions for impact analysis
//!
//! This module contains all the data structures used in the impact analysis system,
//! including both the v1 (legacy) types and new v2 (multi-layer) types.

use crate::model::{LowerBound, SymbolCompact};
use serde::Serialize;
use std::collections::HashMap;

// ============================================================================
// V1 (Legacy) Types - Maintained for backward compatibility
// ============================================================================

/// A step in an impact path showing how symbols are connected
#[derive(Debug, Serialize, Clone)]
pub struct PathStep {
    pub edge_kind: String,
    pub from_symbol: String,
    pub to_symbol: String,
    /// Resolution tier of the traversed edge (issue #62's AC: every
    /// response carrying edges exposes the resolution tier). Absent when
    /// the edge has no resolution kind at all -- a String-Targeted Edge Kind
    /// (Bridge Edge or CONFIG_*) or any edge kind the resolver never labels.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolution_kind: Option<String>,
}

/// Path from seed symbol to impacted symbol
#[derive(Debug, Serialize, Clone)]
pub struct ImpactPath {
    pub steps: Vec<PathStep>,
}

/// Single impacted symbol with relationship details
#[derive(Debug, Serialize)]
pub struct ImpactEntry {
    pub symbol: SymbolCompact,
    pub distance: usize,
    pub relationship: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<ImpactPath>,
    /// Confidence score (0.0-1.0) that this symbol is actually impacted
    /// Added in v2, defaults to 1.0 for v1 compatibility
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
    /// Other paths reaching this symbol (e.g. the same container reached via
    /// a second config URI); `path` is the first.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub also_via: Vec<ImpactPath>,
    /// A JS/TS test attributed to a whole test file (its module symbol), not
    /// one specific test -- see `test_detection::is_file_level_test`.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub file_level: bool,
}

/// Impact grouped by file
#[derive(Debug, Serialize)]
pub struct FileImpact {
    pub path: String,
    pub symbol_count: usize,
    pub symbols: Vec<String>,
}

/// Summary statistics for impact analysis
#[derive(Debug, Serialize)]
pub struct ImpactSummary {
    pub by_file: Vec<FileImpact>,
    pub by_relationship: HashMap<String, usize>,
    pub by_distance: HashMap<usize, usize>,
    pub total_affected: usize,
}

/// Configuration used for the analysis
#[derive(Debug, Serialize)]
pub struct ImpactConfig {
    pub max_depth: usize,
    pub direction: String,
    pub relationship_types: Vec<String>,
    pub include_tests: bool,
    pub limit: usize,
}

/// Result of direct impact analysis
#[derive(Debug, Serialize)]
pub struct ImpactResult {
    pub seeds: Vec<SymbolCompact>,
    pub affected: Vec<ImpactEntry>,
    pub summary: ImpactSummary,
    pub truncated: bool,
    pub config: ImpactConfig,
}

// ============================================================================
// V2 (Multi-Layer) Types - New layered architecture
// ============================================================================

/// Confidence score (0.0-1.0) representing certainty that a symbol is impacted
pub type ConfidenceScore = f32;

/// Source of evidence for impact relationship
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type")]
pub enum ImpactSource {
    /// Direct graph edge (CALL, IMPORT, etc.)
    DirectEdge {
        edge_kind: String,
        distance: usize,
        /// Resolution tier of the traversed edge (issue #62's AC). Absent
        /// when the edge has no resolution kind at all -- a Bridge Edge
        /// kind or any edge kind the resolver never labels.
        #[serde(skip_serializing_if = "Option::is_none")]
        resolution_kind: Option<String>,
    },
    /// Test relationship
    TestLink {
        strategy: TestStrategy,
        test_type: String, // "unit", "integration", "e2e"
        /// Graph hops from the test to the seed (>= 1).
        distance: usize,
    },
    /// Historical co-change pattern
    CoChange {
        frequency: f32,
        co_change_count: usize,
        last_cochange: Option<String>, // ISO timestamp
    },
}

/// How a test reaches the seed through the graph (serialized as the
/// snake_case name).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TestStrategy {
    /// The test imports the seed.
    Import,
    /// The test calls the seed directly.
    Call,
    /// The test calls the interface method the seed implements.
    CallViaInterface,
    /// Any other graph path (transitive callers, bridged edges, ...).
    Graph,
}

impl TestStrategy {
    /// The single mapping from the edge a test hangs off the seed by to a
    /// strategy; only a first-hop edge earns a specific label.
    pub fn from_edge_kind(kind: &str) -> Self {
        match kind {
            "IMPORTS" => Self::Import,
            "CALLS" => Self::Call,
            _ => Self::Graph,
        }
    }
}

/// How a symbol was reached: (parent id, edge kind, resolution kind of the
/// traversed edge, whether the edge was walked against its direction).
pub type ParentLink = (i64, String, Option<String>, bool);

/// Result from a single impact layer
#[derive(Debug, Clone)]
pub struct LayerResult {
    /// Layer name for debugging
    pub layer_name: String,
    /// Impacted symbols with confidence
    pub impacts: Vec<(i64, ConfidenceScore)>, // (symbol_id, confidence)
    /// Evidence for each symbol
    pub evidence: HashMap<i64, Vec<ImpactSource>>,
    /// Execution time in milliseconds
    pub duration_ms: u64,
    /// Whether this layer was truncated
    pub truncated: bool,
    /// Why the layer was truncated, when not a plain size/time limit.
    pub truncation_reason: Option<String>,
    /// Parent tracking for path reconstruction: child_id -> (parent_id,
    /// edge_kind, resolution_kind of the traversed edge, whether the edge was
    /// walked against its direction, i.e. the parent is the edge's target)
    pub parent_map: HashMap<i64, ParentLink>,
    /// Further parent links of nodes re-entered under another config URI
    /// (`parent_map` keeps the first): child_id -> extra links, same shape.
    pub alt_parents: HashMap<i64, Vec<ParentLink>>,
    /// Issue #81 (R5): whether this layer traversed at least one edge with a
    /// heuristic (`bare_name`/`two_segment`) resolution kind. Only the direct
    /// layer (`analyze_direct_impact`) computes this meaningfully; every
    /// other layer reports `false` since they don't walk resolved graph
    /// edges the same way.
    pub traversed_heuristic_kind: bool,
}

/// Configuration for multi-layer impact analysis
#[derive(Debug, Clone)]
pub struct MultiLayerConfig {
    pub direct: DirectConfig,
    pub test: TestConfig,
    pub historical: HistoricalConfig,
    /// Global settings
    pub include_paths: bool,
    pub min_confidence: f32,
    pub limit: usize,
}

impl Default for MultiLayerConfig {
    fn default() -> Self {
        Self {
            direct: DirectConfig::default(),
            test: TestConfig::default(),
            historical: HistoricalConfig::default(),
            include_paths: false,
            min_confidence: 0.0,
            limit: 10000,
        }
    }
}

/// Configuration for direct impact layer (Layer 1)
#[derive(Debug, Clone)]
pub struct DirectConfig {
    pub enabled: bool,
    pub max_depth: usize,
    pub direction: String,  // "upstream", "downstream", "both"
    pub kinds: Vec<String>, // Edge kinds to follow (empty = all)
    /// Resolution kinds to refuse to traverse (issue #81), e.g.
    /// `["bare_name", "two_segment"]`. Empty by default: unchanged behaviour.
    pub exclude_resolution_kinds: Vec<String>,
    pub include_tests: bool,
    pub languages: Option<Vec<String>>,
    /// Config URI the seeds were resolved from (issue #131), if any.
    pub seed_config_uri: Option<String>,
}

impl Default for DirectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_depth: 3,
            direction: "both".to_string(),
            kinds: Vec::new(),
            exclude_resolution_kinds: Vec::new(),
            include_tests: true,
            languages: None,
            seed_config_uri: None,
        }
    }
}

/// Configuration for test impact layer (Layer 2)
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub enabled: bool,
    pub min_priority: f32,       // Minimum test priority to include
    pub test_types: Vec<String>, // "unit", "integration", "e2e" (empty = all)
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            enabled: true, // Enabled by default (Phase 2 complete)
            min_priority: 0.0,
            test_types: Vec::new(),
        }
    }
}

/// Configuration for historical impact layer (Layer 3)
#[derive(Debug, Clone)]
pub struct HistoricalConfig {
    pub enabled: bool,
    /// Minimum co-change occurrences to include
    pub min_occurrences: usize,
    /// Time window in days to look back (max: 365)
    pub time_window_days: i64,
    /// Minimum confidence threshold (co_change_count / min(total_a, total_b))
    pub confidence_threshold: f32,
}

impl Default for HistoricalConfig {
    fn default() -> Self {
        Self {
            enabled: true, // Enabled by default (Phase 3 complete)
            min_occurrences: 3,
            time_window_days: 180, // 6 months
            confidence_threshold: 0.5,
        }
    }
}

/// Result from multi-layer impact analysis
#[derive(Debug, Serialize)]
pub struct UnifiedImpactResult {
    /// Seed symbols
    pub seeds: Vec<SymbolCompact>,
    /// Impacted symbols with combined confidence
    pub affected: Vec<ImpactEntry>,
    /// Summary statistics
    pub summary: ImpactSummary,
    /// Whether results were truncated
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation_reason: Option<String>,
    /// Configuration used
    pub config: ImpactConfig,
    /// Layer-specific metadata
    pub layers: LayerMetadata,
    /// Lower-bound indicator (issue #81) -- set when pending
    /// `unresolved_references` rows touch the direct layer's traversed
    /// symbols. Always `{ is_lower_bound: false, unresolved_count: 0 }`
    /// when the direct layer is disabled, since only that layer traverses
    /// resolvable graph edges.
    pub lower_bound: LowerBound,
    /// Issue #81 (R5): whether the direct layer's own traversal crossed at
    /// least one heuristic (`bare_name`/`two_segment`) edge -- gates the
    /// "retry excluding heuristics" next_hops suggestion in
    /// `handle_analyze_impact`. Internal signal, not part of the response
    /// payload.
    #[serde(skip)]
    pub traversed_heuristic_kind: bool,
}

/// A single entry in a batch impact result
#[derive(Debug, Serialize)]
pub struct BatchImpactEntry {
    pub seed_qualname: String,
    pub seeds: Vec<SymbolCompact>,
    pub affected: Vec<ImpactEntry>,
    pub summary: ImpactSummary,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation_reason: Option<String>,
    pub layers: LayerMetadata,
    pub lower_bound: LowerBound,
    /// Present only when seed resolution failed; contains next_hops and a message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery: Option<serde_json::Value>,
    /// Present only when the TEST layer ran and found no test reaching the
    /// seed through the graph: the reason and follow-up queries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub test_layer: Option<serde_json::Value>,
}

/// Result of batch impact analysis (multiple seeds in one call)
#[derive(Debug, Serialize)]
pub struct BatchImpactResult {
    pub results: Vec<BatchImpactEntry>,
    pub config: ImpactConfig,
    pub total_affected: usize,
    pub total_files: usize,
}

/// Metadata about layer execution
#[derive(Debug, Clone, Serialize)]
pub struct LayerMetadata {
    pub direct: Option<LayerStats>,
    pub test: Option<LayerStats>,
    pub historical: Option<LayerStats>,
}

/// Statistics for a single layer
#[derive(Debug, Clone, Serialize)]
pub struct LayerStats {
    pub enabled: bool,
    pub duration_ms: u64,
    pub result_count: usize,
    pub truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
