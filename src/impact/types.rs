//! Type definitions for impact analysis
//!
//! This module contains all the data structures used in the impact analysis system,
//! including both the v1 (legacy) types and new v2 (multi-layer) types.

use crate::model::SymbolCompact;
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
    },
    /// Test relationship
    TestLink {
        strategy: String, // "import", "call", "naming", "proximity"
        test_type: String, // "unit", "integration", "e2e"
    },
    /// Historical co-change pattern
    CoChange {
        frequency: f32,
        co_change_count: usize,
        last_cochange: Option<String>, // ISO timestamp
    },
}

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
    pub direction: String, // "upstream", "downstream", "both"
    pub kinds: Vec<String>, // Edge kinds to follow (empty = all)
    pub include_tests: bool,
    pub languages: Option<Vec<String>>,
}

impl Default for DirectConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_depth: 3,
            direction: "both".to_string(),
            kinds: Vec::new(),
            include_tests: true,
            languages: None,
        }
    }
}

/// Configuration for test impact layer (Layer 2)
#[derive(Debug, Clone)]
pub struct TestConfig {
    pub enabled: bool,
    pub min_priority: f32, // Minimum test priority to include
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
    /// Configuration used
    pub config: ImpactConfig,
    /// Layer-specific metadata
    pub layers: LayerMetadata,
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
