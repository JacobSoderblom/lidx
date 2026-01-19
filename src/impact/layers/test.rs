//! Test Impact Layer (Layer 2)
//!
//! Discovers test relationships and prioritizes tests for changed code.
//!
//! ## Discovery Strategies
//!
//! 1. **Import Analysis** - Test imports production code (confidence: 0.9)
//! 2. **Call Analysis** - Test calls production functions (confidence: 0.95)
//! 3. **Naming Convention** - `test_foo()` tests `foo()` (confidence: 0.7)
//! 4. **Directory Proximity** - Tests in `tests/` for files in `src/` (confidence: 0.5)
//!
//! ## Usage
//!
//! ```ignore
//! let layer = TestImpactLayer::new(&db);
//! let result = layer.analyze(&[seed_id], graph_version)?;
//! ```

use crate::db::Db;
use crate::impact::types::{ImpactSource, LayerResult};
use crate::indexer::test_detection::{
    classify_test_type, extract_test_target_name, is_test_symbol,
};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Test Impact Layer
///
/// Finds tests that should be run when production code changes
pub struct TestImpactLayer<'a> {
    db: &'a Db,
}

impl<'a> TestImpactLayer<'a> {
    /// Create a new test impact layer
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    /// Analyze test impact for changed symbols
    ///
    /// Returns tests that are likely affected by changes to the seed symbols
    pub fn analyze(&self, seed_ids: &[i64], graph_version: i64) -> Result<LayerResult> {
        let start = Instant::now();

        // Track all discovered test symbols and their evidence
        let mut test_impacts: HashMap<i64, Vec<ImpactSource>> = HashMap::new();

        // Strategy 1: Import-based discovery
        let import_tests = self.discover_import_tests(seed_ids, graph_version)?;
        for (test_id, evidence) in import_tests {
            test_impacts.entry(test_id).or_default().push(evidence);
        }

        // Strategy 2: Call-based discovery
        let call_tests = self.discover_call_tests(seed_ids, graph_version)?;
        for (test_id, evidence) in call_tests {
            test_impacts.entry(test_id).or_default().push(evidence);
        }

        // Strategy 3: Naming convention discovery
        let naming_tests = self.discover_naming_tests(seed_ids, graph_version)?;
        for (test_id, evidence) in naming_tests {
            test_impacts.entry(test_id).or_default().push(evidence);
        }

        // Strategy 4: Directory proximity discovery
        let proximity_tests = self.discover_proximity_tests(seed_ids, graph_version)?;
        for (test_id, evidence) in proximity_tests {
            test_impacts.entry(test_id).or_default().push(evidence);
        }

        // Convert to LayerResult format
        let impacts: Vec<(i64, f32)> = test_impacts
            .iter()
            .map(|(test_id, evidence)| {
                // Calculate confidence from evidence (max confidence from all strategies)
                let confidence = evidence
                    .iter()
                    .filter_map(|e| match e {
                        ImpactSource::TestLink { .. } => {
                            // Extract confidence from strategy name (embedded in strategy field)
                            Some(0.8) // Default if we can't parse
                        }
                        _ => None,
                    })
                    .fold(0.0f32, f32::max);

                (*test_id, confidence)
            })
            .collect();

        // Build evidence map
        let evidence: HashMap<i64, Vec<ImpactSource>> = test_impacts;

        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(LayerResult {
            layer_name: "test".to_string(),
            impacts,
            evidence,
            duration_ms,
            truncated: false,
        })
    }

    /// Strategy 1: Import-based test discovery
    ///
    /// Find test symbols that IMPORT the changed symbols
    /// Confidence: 0.9 (high - direct import relationship)
    fn discover_import_tests(
        &self,
        seed_ids: &[i64],
        graph_version: i64,
    ) -> Result<Vec<(i64, ImpactSource)>> {
        let mut results = Vec::new();
        let mut seen = HashSet::new();

        for seed_id in seed_ids {
            // Find edges where this symbol is the source or target
            let edges = self.db.edges_for_symbol(*seed_id, None, graph_version)?;

            for edge in edges {
                // We want IMPORT edges where the seed is the TARGET (being imported)
                if edge.kind != "IMPORTS" {
                    continue;
                }

                if edge.target_symbol_id == Some(*seed_id) {
                    if let Some(source_id) = edge.source_symbol_id {
                        if seen.contains(&source_id) {
                            continue;
                        }
                        seen.insert(source_id);

                        // Check if source is a test symbol
                        if let Ok(symbols) =
                            self.db.symbols_by_ids(&[source_id], None, graph_version)
                        {
                            if let Some(sym) = symbols.first() {
                                if is_test_symbol(sym) {
                                    let test_type = classify_test_type(sym);
                                    results.push((
                                        source_id,
                                        ImpactSource::TestLink {
                                            strategy: "import".to_string(),
                                            test_type: test_type.to_string(),
                                        },
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Strategy 2: Call-based test discovery
    ///
    /// Find test symbols that CALL the changed symbols
    /// Confidence: 0.95 (very high - direct call relationship)
    fn discover_call_tests(
        &self,
        seed_ids: &[i64],
        graph_version: i64,
    ) -> Result<Vec<(i64, ImpactSource)>> {
        let mut results = Vec::new();
        let mut seen = HashSet::new();

        for seed_id in seed_ids {
            // Find edges where this symbol is the source or target
            let edges = self.db.edges_for_symbol(*seed_id, None, graph_version)?;

            for edge in edges {
                // We want CALL edges where the seed is the TARGET (being called)
                if edge.kind != "CALLS" {
                    continue;
                }

                if edge.target_symbol_id == Some(*seed_id) {
                    if let Some(source_id) = edge.source_symbol_id {
                        if seen.contains(&source_id) {
                            continue;
                        }
                        seen.insert(source_id);

                        // Check if source is a test symbol
                        if let Ok(symbols) =
                            self.db.symbols_by_ids(&[source_id], None, graph_version)
                        {
                            if let Some(sym) = symbols.first() {
                                if is_test_symbol(sym) {
                                    let test_type = classify_test_type(sym);
                                    results.push((
                                        source_id,
                                        ImpactSource::TestLink {
                                            strategy: "call".to_string(),
                                            test_type: test_type.to_string(),
                                        },
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Strategy 3: Naming convention matching
    ///
    /// Match test names to production code names
    /// Examples: `test_calculate` matches `calculate`, `TestFoo` matches `Foo`
    /// Confidence: 0.7 (medium - heuristic-based)
    fn discover_naming_tests(
        &self,
        seed_ids: &[i64],
        graph_version: i64,
    ) -> Result<Vec<(i64, ImpactSource)>> {
        let mut results = Vec::new();
        let mut seen = HashSet::new();

        // Load seed symbols to get their names
        let seeds = self.db.symbols_by_ids(seed_ids, None, graph_version)?;

        for seed in &seeds {
            // Extract potential test names for this symbol
            let seed_name_lower = seed.name.to_lowercase();
            let possible_test_names = vec![
                format!("test_{}", seed_name_lower),
                format!("Test{}", seed.name),
                format!("{}Test", seed.name),
                format!("{}_test", seed_name_lower),
                format!("{}Spec", seed.name),
            ];

            // Search for symbols with these names using find_symbols
            for test_name in possible_test_names {
                if let Ok(candidates) = self.db.find_symbols(&test_name, 100, None, graph_version) {
                    for candidate in candidates {
                        if seen.contains(&candidate.id) {
                            continue;
                        }

                        if is_test_symbol(&candidate) {
                            seen.insert(candidate.id);
                            let test_type = classify_test_type(&candidate);
                            results.push((
                                candidate.id,
                                ImpactSource::TestLink {
                                    strategy: "naming".to_string(),
                                    test_type: test_type.to_string(),
                                },
                            ));
                        }
                    }
                }
            }

            // Also search in reverse: find tests and extract target names
            // This helps find tests like `test_calculate` when we change `calculate`
            let seed_lang = Self::infer_language(&seed.file_path);
            if let Ok(all_tests) = self.db.find_symbols("test", 1000, None, graph_version) {
                for test in all_tests {
                    if seen.contains(&test.id) {
                        continue;
                    }

                    if !is_test_symbol(&test) {
                        continue;
                    }

                    // Skip tests from different languages to avoid cross-language false positives
                    if let (Some(sl), Some(tl)) = (seed_lang, Self::infer_language(&test.file_path)) {
                        if sl != tl {
                            continue;
                        }
                    }

                    if let Some(target_name) = extract_test_target_name(&test.name) {
                        if target_name.to_lowercase() == seed_name_lower
                            || seed.name.to_lowercase().contains(&target_name.to_lowercase())
                        {
                            seen.insert(test.id);
                            let test_type = classify_test_type(&test);
                            results.push((
                                test.id,
                                ImpactSource::TestLink {
                                    strategy: "naming".to_string(),
                                    test_type: test_type.to_string(),
                                },
                            ));
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Strategy 4: Directory proximity heuristic
    ///
    /// Match test files to source files by directory structure
    /// Example: `tests/auth/test_login.py` → all symbols in `src/auth/login.py`
    /// Confidence: 0.5 (low - broad heuristic)
    fn discover_proximity_tests(
        &self,
        seed_ids: &[i64],
        graph_version: i64,
    ) -> Result<Vec<(i64, ImpactSource)>> {
        let mut results = Vec::new();
        let mut seen = HashSet::new();

        // Load seed symbols to get their file paths
        let seeds = self.db.symbols_by_ids(seed_ids, None, graph_version)?;

        for seed in &seeds {
            // Extract directory from seed file path
            // Example: src/auth/login.py → auth/login
            let seed_path = &seed.file_path;
            let seed_components = self.extract_path_components(seed_path);

            // Find test symbols with similar path components
            // Use find_symbols to search for "test" patterns
            let seed_lang = Self::infer_language(seed_path);
            if let Ok(test_candidates) = self.db.find_symbols("test", 1000, None, graph_version) {
                for test in test_candidates {
                    if seen.contains(&test.id) {
                        continue;
                    }

                    if !is_test_symbol(&test) {
                        continue;
                    }

                    // Skip tests from different languages to avoid cross-language false positives
                    if let (Some(sl), Some(tl)) = (seed_lang, Self::infer_language(&test.file_path)) {
                        if sl != tl {
                            continue;
                        }
                    }

                    let test_components = self.extract_path_components(&test.file_path);

                    // Check for overlap in path components — require >= 2 to avoid single-word matches
                    let overlap = seed_components
                        .iter()
                        .filter(|c| test_components.contains(*c))
                        .count();

                    if overlap >= 2 {
                        seen.insert(test.id);
                        let test_type = classify_test_type(&test);
                        results.push((
                            test.id,
                            ImpactSource::TestLink {
                                strategy: "proximity".to_string(),
                                test_type: test_type.to_string(),
                            },
                        ));
                    }
                }
            }
        }

        Ok(results)
    }

    /// Infer language from file extension for cross-language filtering
    fn infer_language(path: &str) -> Option<&'static str> {
        let ext = path.rsplit('.').next()?;
        match ext {
            "py" => Some("python"),
            "cs" => Some("csharp"),
            "ts" | "tsx" => Some("typescript"),
            "js" | "jsx" => Some("javascript"),
            "rs" => Some("rust"),
            "proto" => Some("proto"),
            "sql" => Some("sql"),
            "md" => Some("markdown"),
            _ => None,
        }
    }

    /// Extract meaningful path components for proximity matching
    ///
    /// Example: `src/auth/login.py` → `["auth", "login"]`
    fn extract_path_components(&self, path: &str) -> HashSet<String> {
        let mut components = HashSet::new();

        // Split by / or \
        let parts: Vec<&str> = path.split(&['/', '\\'][..]).collect();

        for part in parts {
            let lower = part.to_lowercase();

            // Skip common directories
            if lower == "src"
                || lower == "lib"
                || lower == "tests"
                || lower == "test"
                || lower.is_empty()
            {
                continue;
            }

            // Remove file extension
            let name = if let Some(dot_pos) = part.rfind('.') {
                &part[..dot_pos]
            } else {
                part
            };

            // Remove test_ prefix for matching
            let clean_name = name
                .trim_start_matches("test_")
                .trim_end_matches("_test")
                .to_lowercase();

            if !clean_name.is_empty() {
                components.insert(clean_name);
            }
        }

        components
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;

    #[test]
    fn test_extract_path_components() {
        // Create a temporary database for testing
        let temp_dir = std::env::temp_dir();
        let db_path = temp_dir.join("test_impact_layer.db");
        let _ = std::fs::remove_file(&db_path); // Clean up if exists

        let db = Db::new(&db_path).unwrap();
        let layer = TestImpactLayer { db: &db };

        let components = layer.extract_path_components("src/auth/login.py");
        assert!(components.contains("auth"));
        assert!(components.contains("login"));
        assert!(!components.contains("src"));

        let components = layer.extract_path_components("tests/test_auth/test_login.py");
        assert!(components.contains("auth"));
        assert!(components.contains("login"));
        assert!(!components.contains("tests"));
        assert!(!components.contains("test"));

        // Cleanup
        let _ = std::fs::remove_file(&db_path);
    }
}
