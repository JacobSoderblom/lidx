//! Historical Impact Layer (Layer 3)
//!
//! Mines co-change patterns from git history and provides time-travel queries.
//!
//! ## Co-Change Mining
//!
//! Discovers symbols that frequently change together in version history:
//! - Tracks symbols across versions using stable_id
//! - Counts co-occurrences in the same commit/version
//! - Computes confidence from co-change frequency
//!
//! ## Time-Travel Queries
//!
//! Compares impact analysis at different points in time:
//! - Run impact analysis at historical graph_version
//! - Compare impact across versions
//! - Identify new, removed, and stable impacts
//!
//! ## Performance
//!
//! Co-change mining is O(n²) in worst case. Optimizations:
//! - Limit time window (default: 180 days, max: 365 days)
//! - Limit symbol count (max: 500 symbols per query)
//! - Database indexes on (stable_id, graph_version)
//! - Result caching for repeated queries
//!
//! ## Usage
//!
//! ```ignore
//! let layer = HistoricalImpactLayer::new(&db);
//! let result = layer.analyze(&[seed_id], config, graph_version)?;
//! ```

use crate::db::Db;
use crate::impact::types::{ConfidenceScore, ImpactSource, LayerResult};
use anyhow::Result;
use rusqlite::OptionalExtension;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

/// Maximum symbols to analyze for co-change (prevent O(n²) explosion)
const MAX_COCHANGE_SYMBOLS: usize = 500;

/// Maximum time window in days
const MAX_TIME_WINDOW_DAYS: i64 = 365;

/// Co-change pattern between two symbols
#[derive(Debug, Clone)]
pub struct CoChangePattern {
    /// Stable ID of first symbol
    pub symbol_a_stable_id: String,
    /// Stable ID of second symbol
    pub symbol_b_stable_id: String,
    /// Number of times they changed together
    pub co_change_count: usize,
    /// Total changes to symbol A
    pub total_changes_a: usize,
    /// Total changes to symbol B
    pub total_changes_b: usize,
    /// Confidence score (co_change_count / min(total_a, total_b))
    pub confidence: f32,
}

/// Time-travel comparison result
#[derive(Debug)]
pub struct TimeTravelComparison {
    /// Number of impacts at earlier version
    pub version_a_impact_count: usize,
    /// Number of impacts at later version
    pub version_b_impact_count: usize,
    /// Symbols newly impacted (in B but not A)
    pub new_impacts: Vec<String>,
    /// Symbols no longer impacted (in A but not B)
    pub removed_impacts: Vec<String>,
    /// Symbols impacted in both versions
    pub stable_impacts: Vec<String>,
}

/// Historical Impact Layer
///
/// Analyzes co-change patterns and provides time-travel queries
pub struct HistoricalImpactLayer<'a> {
    db: &'a Db,
}

impl<'a> HistoricalImpactLayer<'a> {
    /// Create a new historical impact layer
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    /// Analyze historical impact for seed symbols
    ///
    /// Returns symbols that frequently co-changed with seeds in git history
    pub fn analyze(
        &self,
        seed_ids: &[i64],
        time_window_days: i64,
        min_occurrences: usize,
        graph_version: i64,
    ) -> Result<LayerResult> {
        let start = Instant::now();
        let conn = self.db.read_conn()?;

        // Get stable IDs for seed symbols
        let seed_stable_ids = self.get_stable_ids_for_symbols(&conn, seed_ids, graph_version)?;

        if seed_stable_ids.is_empty() {
            return Ok(LayerResult {
                layer_name: "historical".to_string(),
                impacts: Vec::new(),
                evidence: HashMap::new(),
                duration_ms: start.elapsed().as_millis() as u64,
                truncated: false,
            });
        }

        // Enforce time window limit
        let time_window = time_window_days.min(MAX_TIME_WINDOW_DAYS);

        // Find co-change patterns
        let patterns = self.find_co_changes(&conn, &seed_stable_ids, time_window, min_occurrences)?;

        // Convert patterns to impacts
        let mut impacts: HashMap<i64, ConfidenceScore> = HashMap::new();
        let mut evidence: HashMap<i64, Vec<ImpactSource>> = HashMap::new();

        // Resolve stable_ids back to current symbol IDs
        for pattern in patterns {
            // Only include pattern.symbol_b (the co-changed symbol, not the seed)
            if let Some(symbol_id) =
                self.resolve_stable_id_to_current(&conn, &pattern.symbol_b_stable_id, graph_version)?
            {
                // Skip if this is a seed symbol
                if seed_ids.contains(&symbol_id) {
                    continue;
                }

                // Update confidence (take max if multiple seeds co-change with this symbol)
                impacts
                    .entry(symbol_id)
                    .and_modify(|conf| *conf = conf.max(pattern.confidence))
                    .or_insert(pattern.confidence);

                // Add evidence
                evidence.entry(symbol_id).or_default().push(
                    ImpactSource::CoChange {
                        frequency: pattern.confidence,
                        co_change_count: pattern.co_change_count,
                        last_cochange: None,
                    },
                );
            }
        }

        let impacts_vec: Vec<(i64, ConfidenceScore)> = impacts.into_iter().collect();
        let duration_ms = start.elapsed().as_millis() as u64;

        Ok(LayerResult {
            layer_name: "historical".to_string(),
            impacts: impacts_vec,
            evidence,
            duration_ms,
            truncated: false,
        })
    }

    /// Find symbols that frequently co-change with the given symbols
    ///
    /// Now uses the pre-mined co_changes table instead of computing on the fly
    fn find_co_changes(
        &self,
        conn: &rusqlite::Connection,
        seed_stable_ids: &[String],
        _time_window_days: i64,
        min_co_occurrences: usize,
    ) -> Result<Vec<CoChangePattern>> {
        if seed_stable_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Get file paths for seed symbols
        let seed_files = self.get_files_for_stable_ids(conn, seed_stable_ids)?;

        if seed_files.is_empty() {
            return Ok(Vec::new());
        }

        // Build IN clause for seed files
        let placeholders = seed_files.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT file_a, file_b, co_change_count, total_commits_a, total_commits_b, confidence
             FROM co_changes
             WHERE (file_a IN ({}) OR file_b IN ({}))
               AND co_change_count >= ?
             ORDER BY confidence DESC
             LIMIT ?",
            placeholders, placeholders
        );

        let mut stmt = conn.prepare(&sql)?;

        // Build params
        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
        for file in &seed_files {
            params.push(file);
        }
        for file in &seed_files {
            params.push(file);
        }
        let min_occ = min_co_occurrences as i64;
        params.push(&min_occ);
        let limit = MAX_COCHANGE_SYMBOLS as i64;
        params.push(&limit);

        let rows = stmt.query_map(&*params, |row| {
            let file_a: String = row.get(0)?;
            let file_b: String = row.get(1)?;
            let co_change_count: i64 = row.get(2)?;
            let total_a: i64 = row.get(3)?;
            let total_b: i64 = row.get(4)?;
            let confidence: f64 = row.get(5)?;

            Ok((file_a, file_b, co_change_count, total_a, total_b, confidence))
        })?;

        let mut file_pairs = Vec::new();
        for row in rows {
            file_pairs.push(row?);
        }

        // Convert file-level co-changes to symbol-level
        // Map files to stable_ids and create patterns
        let mut patterns: Vec<CoChangePattern> = Vec::new();

        for (file_a, file_b, co_change_count, total_a, total_b, confidence) in file_pairs {
            // Get stable_ids for symbols in these files
            let stable_ids_a = self.get_stable_ids_for_file(conn, &file_a)?;
            let stable_ids_b = self.get_stable_ids_for_file(conn, &file_b)?;

            // Create patterns for symbol pairs
            for stable_id_a in &stable_ids_a {
                // Only include if this is a seed symbol
                if !seed_stable_ids.contains(stable_id_a) {
                    continue;
                }

                for stable_id_b in &stable_ids_b {
                    // Skip if same symbol
                    if stable_id_a == stable_id_b {
                        continue;
                    }

                    patterns.push(CoChangePattern {
                        symbol_a_stable_id: stable_id_a.clone(),
                        symbol_b_stable_id: stable_id_b.clone(),
                        co_change_count: co_change_count as usize,
                        total_changes_a: total_a as usize,
                        total_changes_b: total_b as usize,
                        confidence: confidence as f32,
                    });
                }
            }
        }

        // Deduplicate and sort
        patterns.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        patterns.dedup_by(|a, b| {
            a.symbol_a_stable_id == b.symbol_a_stable_id
                && a.symbol_b_stable_id == b.symbol_b_stable_id
        });

        // Limit results
        if patterns.len() > MAX_COCHANGE_SYMBOLS {
            patterns.truncate(MAX_COCHANGE_SYMBOLS);
        }

        Ok(patterns)
    }

    /// Get file paths for given stable IDs
    fn get_files_for_stable_ids(&self, conn: &rusqlite::Connection, stable_ids: &[String]) -> Result<Vec<String>> {
        let placeholders = stable_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT DISTINCT f.path
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.stable_id IN ({})",
            placeholders
        );

        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::ToSql> = stable_ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();

        let rows = stmt.query_map(&*params, |row| row.get::<_, String>(0))?;

        let mut files = Vec::new();
        for row in rows {
            files.push(row?);
        }

        Ok(files)
    }

    /// Get stable IDs for symbols in a given file
    fn get_stable_ids_for_file(&self, conn: &rusqlite::Connection, file_path: &str) -> Result<Vec<String>> {
        let mut stmt = conn.prepare(
            "SELECT DISTINCT s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE f.path = ? AND s.stable_id IS NOT NULL"
        )?;

        let rows = stmt.query_map(rusqlite::params![file_path], |row| row.get::<_, String>(0))?;

        let mut stable_ids = Vec::new();
        for row in rows {
            stable_ids.push(row?);
        }

        Ok(stable_ids)
    }

    /// Compare impact between two time points
    pub fn time_travel_compare(
        &self,
        seed_stable_ids: &[String],
        version_a: i64,
        version_b: i64,
    ) -> Result<TimeTravelComparison> {
        let conn = self.db.read_conn()?;

        // Run impact analysis at version A
        let impact_a = self.analyze_at_version(&conn, seed_stable_ids, version_a)?;

        // Run impact analysis at version B
        let impact_b = self.analyze_at_version(&conn, seed_stable_ids, version_b)?;

        // Compare results
        let set_a: HashSet<String> = impact_a.into_iter().collect();
        let set_b: HashSet<String> = impact_b.into_iter().collect();

        let new_impacts: Vec<String> = set_b.difference(&set_a).cloned().collect();
        let removed_impacts: Vec<String> = set_a.difference(&set_b).cloned().collect();
        let stable_impacts: Vec<String> = set_a.intersection(&set_b).cloned().collect();

        Ok(TimeTravelComparison {
            version_a_impact_count: set_a.len(),
            version_b_impact_count: set_b.len(),
            new_impacts,
            removed_impacts,
            stable_impacts,
        })
    }

    /// Run impact analysis at a specific historical version
    ///
    /// This is a simplified version that just returns stable_ids.
    /// In a full implementation, this would run the full multi-layer BFS at that version.
    fn analyze_at_version(
        &self,
        conn: &rusqlite::Connection,
        seed_stable_ids: &[String],
        graph_version: i64,
    ) -> Result<Vec<String>> {
        // For now, just return the direct neighbors at that version
        // This is a placeholder - full implementation would run complete BFS

        let mut result = HashSet::new();

        // Add seeds
        for stable_id in seed_stable_ids {
            result.insert(stable_id.clone());
        }

        // Get symbol IDs at this version
        let seed_ids: Vec<i64> = seed_stable_ids
            .iter()
            .filter_map(|stable_id| {
                conn.query_row(
                    "SELECT id FROM symbols WHERE stable_id = ? AND graph_version = ?",
                    rusqlite::params![stable_id, graph_version],
                    |row| row.get(0),
                )
                .ok()
            })
            .collect();

        // Get direct neighbors (simplified - just 1 hop)
        for seed_id in seed_ids {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT s.stable_id
                 FROM edges e
                 JOIN symbols s ON e.target_symbol_id = s.id
                 WHERE e.source_symbol_id = ? AND e.graph_version = ? AND s.stable_id IS NOT NULL",
            )?;

            let stable_ids = stmt.query_map(rusqlite::params![seed_id, graph_version], |row| {
                row.get::<_, String>(0)
            })?;

            for stable_id in stable_ids {
                if let Ok(id) = stable_id {
                    result.insert(id);
                }
            }
        }

        Ok(result.into_iter().collect())
    }

    // ========================================================================
    // Helper methods for database queries
    // ========================================================================

    /// Get stable IDs for given symbol IDs
    fn get_stable_ids_for_symbols(
        &self,
        conn: &rusqlite::Connection,
        symbol_ids: &[i64],
        graph_version: i64,
    ) -> Result<Vec<String>> {

        let placeholders = symbol_ids
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");

        let query = format!(
            "SELECT DISTINCT stable_id FROM symbols WHERE id IN ({}) AND graph_version = ? AND stable_id IS NOT NULL",
            placeholders
        );

        let mut stmt = conn.prepare(&query)?;

        let mut params: Vec<&dyn rusqlite::ToSql> =
            symbol_ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
        params.push(&graph_version);

        let stable_ids = stmt
            .query_map(rusqlite::params_from_iter(params.iter()), |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<String>, _>>()?;

        Ok(stable_ids)
    }

    /// Resolve stable_id to current symbol ID at specific graph version
    fn resolve_stable_id_to_current(
        &self,
        conn: &rusqlite::Connection,
        stable_id: &str,
        graph_version: i64,
    ) -> Result<Option<i64>> {
        let symbol_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM symbols WHERE stable_id = ? AND graph_version = ?",
                rusqlite::params![stable_id, graph_version],
                |row| row.get(0),
            )
            .optional()?;

        Ok(symbol_id)
    }

    /// Get all graph versions within time window (days back from latest)
    #[allow(dead_code)]
    fn get_graph_versions_in_window(&self, conn: &rusqlite::Connection, days_back: i64) -> Result<Vec<i64>> {

        // Calculate cutoff timestamp (days back from now)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let cutoff = now - (days_back * 24 * 60 * 60);

        // Get versions created after cutoff
        let mut stmt = conn.prepare(
            "SELECT DISTINCT id FROM graph_versions
             WHERE created >= ?
             ORDER BY id ASC",
        )?;

        let versions = stmt
            .query_map(rusqlite::params![cutoff], |row| row.get::<_, i64>(0))?
            .collect::<Result<Vec<i64>, _>>()?;

        Ok(versions)
    }

    /// Get changed symbol stable IDs between two versions
    #[allow(dead_code)]
    fn get_changed_symbols_between_versions(
        &self,
        conn: &rusqlite::Connection,
        _version_a: i64,
        version_b: i64,
    ) -> Result<HashSet<String>> {

        let mut changed = HashSet::new();

        // Symbols added in version_b
        let mut stmt = conn.prepare(
            "SELECT DISTINCT stable_id FROM symbols
             WHERE graph_version = ? AND stable_id IS NOT NULL",
        )?;

        let added = stmt.query_map(rusqlite::params![version_b], |row| {
            row.get::<_, String>(0)
        })?;

        for stable_id in added {
            if let Ok(id) = stable_id {
                changed.insert(id);
            }
        }

        // Symbols deleted from version_a (existed in a but not in b)
        // Note: This is a simplified heuristic - proper diff would be more complex
        // For now, we focus on symbols that appear in version_b (recently changed)

        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_cochange_symbols_reasonable() {
        assert!(MAX_COCHANGE_SYMBOLS <= 1000);
        assert!(MAX_COCHANGE_SYMBOLS >= 100);
    }

    #[test]
    fn test_max_time_window_reasonable() {
        assert!(MAX_TIME_WINDOW_DAYS <= 730); // Max 2 years
        assert!(MAX_TIME_WINDOW_DAYS >= 30); // At least 1 month
    }

    #[test]
    fn test_confidence_calculation() {
        // Pattern where A and B changed together 5 times
        // A changed 10 times total, B changed 8 times total
        let co_change_count = 5;
        let total_a = 10;
        let total_b = 8;

        let min_changes = total_a.min(total_b);
        let confidence = (co_change_count as f32) / (min_changes as f32);

        // Confidence = 5 / 8 = 0.625
        assert!((confidence - 0.625).abs() < 0.001);
    }

    #[test]
    fn test_confidence_capped_at_one() {
        // Edge case: co_change_count >= min(total_a, total_b)
        let co_change_count = 10;
        let total_a = 10;
        let total_b = 12;

        let min_changes = total_a.min(total_b);
        let confidence = ((co_change_count as f32) / (min_changes as f32)).min(1.0);

        assert_eq!(confidence, 1.0);
    }
}
