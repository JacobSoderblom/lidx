use crate::indexer::differ::SymbolDiff;
use std::time::{Duration, Instant};

/// Configuration for batch writing operations
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of files to collect before flushing (default: 100)
    pub batch_size: usize,
    /// Maximum time to wait before flushing (default: 500ms)
    pub flush_interval: Duration,
    /// Maximum approximate memory for pending diffs (default: 10MB)
    pub max_memory_bytes: usize,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            batch_size: std::env::var("LIDX_BATCH_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100),
            flush_interval: Duration::from_millis(
                std::env::var("LIDX_FLUSH_INTERVAL_MS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(500),
            ),
            max_memory_bytes: std::env::var("LIDX_MAX_MEMORY_MB")
                .ok()
                .and_then(|s| s.parse::<usize>().ok())
                .map(|mb| mb * 1024 * 1024)
                .unwrap_or(10 * 1024 * 1024), // 10MB default
        }
    }
}

/// File diff entry for batch processing
#[derive(Debug, Clone)]
pub struct FileDiff {
    pub file_id: i64,
    pub file_path: String,
    pub diff: SymbolDiff,
    pub graph_version: i64,
    pub commit_sha: Option<String>,
}

/// Batch writer for collecting symbol diffs from multiple files
///
/// This writer collects diffs from multiple files and uses three triggers
/// to decide when to flush:
///
/// 1. **Batch size:** When pending_diffs reaches batch_size
/// 2. **Time interval:** When flush_interval has elapsed since last flush
/// 3. **Memory pressure:** When estimated memory usage exceeds max_memory_bytes
///
/// # Performance
///
/// - Individual transactions: 100 files = 100 transactions (~200 files/sec)
/// - Batch transactions: 100 files = 1 transaction (>500 files/sec target)
///
/// # Usage
///
/// ```rust
/// let mut writer = BatchWriter::new(BatchConfig::default());
///
/// // Collect diffs from multiple files
/// for file in files {
///     let diff = compute_diff(file);
///     writer.add(FileDiff { ... });
///
///     if writer.should_flush() {
///         let batch = writer.take();
///         db.update_files_symbols_batch(&batch)?;
///     }
/// }
///
/// // Flush remaining diffs
/// if !writer.is_empty() {
///     let batch = writer.take();
///     db.update_files_symbols_batch(&batch)?;
/// }
/// ```
pub struct BatchWriter {
    pending_diffs: Vec<FileDiff>,
    config: BatchConfig,
    last_flush: Instant,
    estimated_memory_bytes: usize,
}

impl BatchWriter {
    /// Create a new batch writer with the given configuration
    pub fn new(config: BatchConfig) -> Self {
        Self {
            pending_diffs: Vec::new(),
            config,
            last_flush: Instant::now(),
            estimated_memory_bytes: 0,
        }
    }

    /// Create a new batch writer with default configuration
    pub fn with_defaults() -> Self {
        Self::new(BatchConfig::default())
    }

    /// Add a file diff to the batch
    pub fn add(&mut self, file_diff: FileDiff) {
        // Estimate memory for this diff
        let diff_memory = Self::estimate_diff_memory(&file_diff.diff);
        self.estimated_memory_bytes += diff_memory;
        self.pending_diffs.push(file_diff);
    }

    /// Check if we should flush based on configured triggers
    pub fn should_flush(&self) -> bool {
        if self.pending_diffs.is_empty() {
            return false;
        }

        // Trigger 1: Batch size reached
        if self.pending_diffs.len() >= self.config.batch_size {
            return true;
        }

        // Trigger 2: Flush interval elapsed
        if self.last_flush.elapsed() >= self.config.flush_interval {
            return true;
        }

        // Trigger 3: Memory limit exceeded
        if self.estimated_memory_bytes >= self.config.max_memory_bytes {
            return true;
        }

        false
    }

    /// Take all pending diffs and reset the writer
    ///
    /// This returns the pending diffs and resets the internal state,
    /// ready for the next batch.
    pub fn take(&mut self) -> Vec<FileDiff> {
        self.last_flush = Instant::now();
        self.estimated_memory_bytes = 0;
        std::mem::take(&mut self.pending_diffs)
    }

    /// Estimate memory usage for a diff
    ///
    /// This is a rough estimate used to prevent OOM on huge batches.
    /// We estimate:
    /// - 200 bytes per symbol (qualname, signature, docstring, etc.)
    /// - 50 bytes per deleted stable_id
    fn estimate_diff_memory(diff: &SymbolDiff) -> usize {
        let added_memory = diff.added.len() * 200;
        let modified_memory = diff.modified.len() * 200;
        let deleted_memory = diff.deleted.len() * 50;
        added_memory + modified_memory + deleted_memory
    }

    /// Get number of pending diffs
    pub fn pending_count(&self) -> usize {
        self.pending_diffs.len()
    }

    /// Get estimated memory usage in bytes
    pub fn estimated_memory(&self) -> usize {
        self.estimated_memory_bytes
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.pending_diffs.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::extract::SymbolInput;

    #[test]
    fn test_batch_config_defaults() {
        let config = BatchConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.flush_interval, Duration::from_millis(500));
        assert_eq!(config.max_memory_bytes, 10 * 1024 * 1024);
    }

    #[test]
    fn test_should_flush_on_batch_size() {
        let config = BatchConfig {
            batch_size: 2,
            flush_interval: Duration::from_secs(9999),
            max_memory_bytes: usize::MAX,
        };
        let mut writer = BatchWriter::new(config);

        // Add first diff - should not flush
        writer.add(FileDiff {
            file_id: 1,
            file_path: "test.py".to_string(),
            diff: SymbolDiff::default(),
            graph_version: 1,
            commit_sha: None,
        });
        assert!(!writer.should_flush());

        // Add second diff - should flush
        writer.add(FileDiff {
            file_id: 2,
            file_path: "test2.py".to_string(),
            diff: SymbolDiff::default(),
            graph_version: 1,
            commit_sha: None,
        });
        assert!(writer.should_flush());
    }

    #[test]
    fn test_should_flush_on_timeout() {
        let config = BatchConfig {
            batch_size: 1000,
            flush_interval: Duration::from_millis(10),
            max_memory_bytes: usize::MAX,
        };
        let mut writer = BatchWriter::new(config);

        // Add a diff
        writer.add(FileDiff {
            file_id: 1,
            file_path: "test.py".to_string(),
            diff: SymbolDiff::default(),
            graph_version: 1,
            commit_sha: None,
        });

        // Should not flush immediately
        assert!(!writer.should_flush());

        // Wait for interval
        std::thread::sleep(Duration::from_millis(15));

        // Should flush now
        assert!(writer.should_flush());
    }

    #[test]
    fn test_should_flush_on_memory_limit() {
        let config = BatchConfig {
            batch_size: 1000,
            flush_interval: Duration::from_secs(9999),
            max_memory_bytes: 1000, // Very small limit
        };
        let mut writer = BatchWriter::new(config);

        // Add diff with many symbols
        let mut diff = SymbolDiff::default();
        for i in 0..10 {
            diff.added.push(SymbolInput {
                kind: "function".to_string(),
                name: format!("func{}", i),
                qualname: format!("module.func{}", i),
                start_line: i,
                start_col: 0,
                end_line: i + 1,
                end_col: 0,
                start_byte: 0,
                end_byte: 0,
                signature: Some(format!("() -> None")),
                docstring: Some(format!("Docstring for func{}", i)),
            });
        }

        writer.add(FileDiff {
            file_id: 1,
            file_path: "test.py".to_string(),
            diff,
            graph_version: 1,
            commit_sha: None,
        });

        // Should trigger memory limit flush
        assert!(writer.should_flush());
    }

    #[test]
    fn test_take_resets_state() {
        let mut writer = BatchWriter::with_defaults();

        // Add some diffs with actual symbols
        let mut diff = SymbolDiff::default();
        diff.added.push(SymbolInput {
            kind: "function".to_string(),
            name: "test".to_string(),
            qualname: "module.test".to_string(),
            start_line: 1,
            start_col: 0,
            end_line: 10,
            end_col: 0,
            start_byte: 0,
            end_byte: 0,
            signature: Some("() -> None".to_string()),
            docstring: Some("Test function".to_string()),
        });

        writer.add(FileDiff {
            file_id: 1,
            file_path: "test.py".to_string(),
            diff,
            graph_version: 1,
            commit_sha: None,
        });

        assert_eq!(writer.pending_count(), 1);
        assert!(writer.estimated_memory() > 0);

        // Take the batch
        let batch = writer.take();
        assert_eq!(batch.len(), 1);

        // Writer should be reset
        assert_eq!(writer.pending_count(), 0);
        assert_eq!(writer.estimated_memory(), 0);
        assert!(writer.is_empty());
    }

    #[test]
    fn test_estimate_diff_memory() {
        let diff = SymbolDiff {
            added: vec![SymbolInput {
                kind: "function".to_string(),
                name: "test".to_string(),
                qualname: "module.test".to_string(),
                start_line: 1,
                start_col: 0,
                end_line: 10,
                end_col: 0,
                start_byte: 0,
                end_byte: 0,
                signature: Some("() -> None".to_string()),
                docstring: Some("Test function".to_string()),
            }],
            modified: vec![SymbolInput {
                kind: "function".to_string(),
                name: "test2".to_string(),
                qualname: "module.test2".to_string(),
                start_line: 1,
                start_col: 0,
                end_line: 10,
                end_col: 0,
                start_byte: 0,
                end_byte: 0,
                signature: Some("() -> None".to_string()),
                docstring: Some("Test function 2".to_string()),
            }],
            deleted: vec!["sym_abc123".to_string(), "sym_def456".to_string()],
            unchanged: vec![],
        };

        let memory = BatchWriter::estimate_diff_memory(&diff);
        // 1 added * 200 + 1 modified * 200 + 2 deleted * 50 = 500
        assert_eq!(memory, 500);
    }

    #[test]
    fn test_empty_batch_should_not_flush() {
        let writer = BatchWriter::with_defaults();
        assert!(!writer.should_flush());
    }

    // ===== Phase 6: Batch Edge Case Tests =====

    #[test]
    fn test_edge_case_single_file_batch() {
        // Test: Batch with only one file (no batching benefit but should work)
        let mut writer = BatchWriter::with_defaults();

        let diff = SymbolDiff {
            added: vec![SymbolInput {
                kind: "function".to_string(),
                name: "foo".to_string(),
                qualname: "test.foo".to_string(),
                start_line: 1,
                start_col: 0,
                end_line: 5,
                end_col: 0,
                start_byte: 0,
                end_byte: 0,
                signature: Some("()".to_string()),
                docstring: None,
            }],
            modified: vec![],
            deleted: vec![],
            unchanged: vec![],
        };

        writer.add(FileDiff {
            file_id: 1,
            file_path: "single.py".to_string(),
            diff,
            graph_version: 1,
            commit_sha: None,
        });

        assert_eq!(writer.pending_count(), 1);
        let batch = writer.take();
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn test_edge_case_large_batch() {
        // Test: Batch with 1000+ files
        let mut writer = BatchWriter::with_defaults();

        for i in 0..1500 {
            let diff = SymbolDiff {
                added: vec![SymbolInput {
                    kind: "function".to_string(),
                    name: format!("func{}", i),
                    qualname: format!("module{}.func{}", i, i),
                    start_line: 1,
                    start_col: 0,
                    end_line: 5,
                    end_col: 0,
                    start_byte: 0,
                    end_byte: 0,
                    signature: Some("()".to_string()),
                    docstring: None,
                }],
                modified: vec![],
                deleted: vec![],
                unchanged: vec![],
            };

            writer.add(FileDiff {
                file_id: i,
                file_path: format!("file{}.py", i),
                diff,
                graph_version: 1,
                commit_sha: None,
            });
        }

        assert_eq!(writer.pending_count(), 1500);
        let batch = writer.take();
        assert_eq!(batch.len(), 1500);
    }

    #[test]
    fn test_edge_case_batch_all_deletes() {
        // Test: Batch where all files have only deletions
        let mut writer = BatchWriter::with_defaults();

        for i in 0..10 {
            let diff = SymbolDiff {
                added: vec![],
                modified: vec![],
                deleted: vec![format!("sym_deleted{}_1", i), format!("sym_deleted{}_2", i)],
                unchanged: vec![],
            };

            writer.add(FileDiff {
                file_id: i,
                file_path: format!("file{}.py", i),
                diff,
                graph_version: 1,
                commit_sha: None,
            });
        }

        assert_eq!(writer.pending_count(), 10);
        let batch = writer.take();
        assert_eq!(batch.len(), 10);

        // Verify all diffs are delete-only
        for file_diff in &batch {
            assert!(file_diff.diff.added.is_empty());
            assert!(file_diff.diff.modified.is_empty());
            assert_eq!(file_diff.diff.deleted.len(), 2);
        }
    }

    #[test]
    fn test_edge_case_batch_all_inserts() {
        // Test: Batch where all files have only insertions (new files)
        let mut writer = BatchWriter::with_defaults();

        for i in 0..10 {
            let diff = SymbolDiff {
                added: vec![SymbolInput {
                    kind: "function".to_string(),
                    name: format!("func{}", i),
                    qualname: format!("module{}.func{}", i, i),
                    start_line: 1,
                    start_col: 0,
                    end_line: 5,
                    end_col: 0,
                    start_byte: 0,
                    end_byte: 0,
                    signature: Some("()".to_string()),
                    docstring: None,
                }],
                modified: vec![],
                deleted: vec![],
                unchanged: vec![],
            };

            writer.add(FileDiff {
                file_id: i,
                file_path: format!("new_file{}.py", i),
                diff,
                graph_version: 1,
                commit_sha: None,
            });
        }

        assert_eq!(writer.pending_count(), 10);
        let batch = writer.take();

        // Verify all diffs are insert-only
        for file_diff in &batch {
            assert_eq!(file_diff.diff.added.len(), 1);
            assert!(file_diff.diff.modified.is_empty());
            assert!(file_diff.diff.deleted.is_empty());
        }
    }

    #[test]
    fn test_edge_case_batch_mixed_operations() {
        // Test: Batch with mix of adds, modifies, deletes across different files
        let mut writer = BatchWriter::with_defaults();

        // File 1: Only adds
        writer.add(FileDiff {
            file_id: 1,
            file_path: "adds.py".to_string(),
            diff: SymbolDiff {
                added: vec![SymbolInput {
                    kind: "function".to_string(),
                    name: "new_func".to_string(),
                    qualname: "adds.new_func".to_string(),
                    start_line: 1,
                    start_col: 0,
                    end_line: 5,
                    end_col: 0,
                    start_byte: 0,
                    end_byte: 0,
                    signature: Some("()".to_string()),
                    docstring: None,
                }],
                modified: vec![],
                deleted: vec![],
                unchanged: vec![],
            },
            graph_version: 1,
            commit_sha: None,
        });

        // File 2: Only modifies
        writer.add(FileDiff {
            file_id: 2,
            file_path: "mods.py".to_string(),
            diff: SymbolDiff {
                added: vec![],
                modified: vec![SymbolInput {
                    kind: "function".to_string(),
                    name: "changed_func".to_string(),
                    qualname: "mods.changed_func".to_string(),
                    start_line: 10,
                    start_col: 0,
                    end_line: 15,
                    end_col: 0,
                    start_byte: 0,
                    end_byte: 0,
                    signature: Some("()".to_string()),
                    docstring: None,
                }],
                deleted: vec![],
                unchanged: vec![],
            },
            graph_version: 1,
            commit_sha: None,
        });

        // File 3: Only deletes
        writer.add(FileDiff {
            file_id: 3,
            file_path: "dels.py".to_string(),
            diff: SymbolDiff {
                added: vec![],
                modified: vec![],
                deleted: vec!["sym_removed".to_string()],
                unchanged: vec![],
            },
            graph_version: 1,
            commit_sha: None,
        });

        // File 4: Mix of all
        writer.add(FileDiff {
            file_id: 4,
            file_path: "mixed.py".to_string(),
            diff: SymbolDiff {
                added: vec![SymbolInput {
                    kind: "function".to_string(),
                    name: "added".to_string(),
                    qualname: "mixed.added".to_string(),
                    start_line: 1,
                    start_col: 0,
                    end_line: 5,
                    end_col: 0,
                    start_byte: 0,
                    end_byte: 0,
                    signature: Some("()".to_string()),
                    docstring: None,
                }],
                modified: vec![SymbolInput {
                    kind: "function".to_string(),
                    name: "modified".to_string(),
                    qualname: "mixed.modified".to_string(),
                    start_line: 10,
                    start_col: 0,
                    end_line: 15,
                    end_col: 0,
                    start_byte: 0,
                    end_byte: 0,
                    signature: Some("()".to_string()),
                    docstring: None,
                }],
                deleted: vec!["sym_mixed_deleted".to_string()],
                unchanged: vec![],
            },
            graph_version: 1,
            commit_sha: None,
        });

        assert_eq!(writer.pending_count(), 4);
        let batch = writer.take();
        assert_eq!(batch.len(), 4);

        // Verify each file has expected operations
        assert_eq!(batch[0].diff.added.len(), 1);
        assert_eq!(batch[1].diff.modified.len(), 1);
        assert_eq!(batch[2].diff.deleted.len(), 1);
        assert_eq!(batch[3].diff.added.len(), 1);
        assert_eq!(batch[3].diff.modified.len(), 1);
        assert_eq!(batch[3].diff.deleted.len(), 1);
    }

    #[test]
    fn test_edge_case_empty_diffs_in_batch() {
        // Test: Some files with no changes (empty diffs)
        let mut writer = BatchWriter::with_defaults();

        for i in 0..5 {
            writer.add(FileDiff {
                file_id: i,
                file_path: format!("file{}.py", i),
                diff: SymbolDiff::default(), // Empty diff
                graph_version: 1,
                commit_sha: None,
            });
        }

        assert_eq!(writer.pending_count(), 5);
        let batch = writer.take();

        // All diffs should be empty
        for file_diff in &batch {
            assert!(file_diff.diff.added.is_empty());
            assert!(file_diff.diff.modified.is_empty());
            assert!(file_diff.diff.deleted.is_empty());
            assert!(file_diff.diff.unchanged.is_empty());
        }
    }

    #[test]
    fn test_edge_case_memory_estimation_accuracy() {
        // Test: Verify memory estimation is reasonably accurate
        let mut writer = BatchWriter::with_defaults();

        let diff = SymbolDiff {
            added: vec![SymbolInput {
                kind: "function".to_string(),
                name: "a".repeat(100), // Large name
                qualname: "module.".to_string() + &"a".repeat(100),
                start_line: 1,
                start_col: 0,
                end_line: 5,
                end_col: 0,
                start_byte: 0,
                end_byte: 0,
                signature: Some("x".repeat(200)), // Large signature
                docstring: Some("y".repeat(500)), // Large docstring
            }],
            modified: vec![],
            deleted: vec![],
            unchanged: vec![],
        };

        let estimated_before = writer.estimated_memory();
        writer.add(FileDiff {
            file_id: 1,
            file_path: "large.py".to_string(),
            diff,
            graph_version: 1,
            commit_sha: None,
        });
        let estimated_after = writer.estimated_memory();

        // Memory should have increased by at least the base estimate (200 bytes)
        assert!(estimated_after > estimated_before);
        assert!(estimated_after >= 200);
    }
}
