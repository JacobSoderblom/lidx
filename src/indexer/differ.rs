use crate::indexer::extract::SymbolInput;
use crate::indexer::stable_id::compute_stable_symbol_id;
use crate::model::Symbol;
use std::collections::HashMap;

/// Result of comparing old symbols (database) vs new symbols (just extracted)
#[derive(Debug, Default, Clone)]
pub struct SymbolDiff {
    /// Symbols that are new (not in database)
    pub added: Vec<SymbolInput>,
    /// Symbols that exist but have changed content (line numbers, docstring)
    pub modified: Vec<SymbolInput>,
    /// Stable IDs of symbols that no longer exist in the file
    pub deleted: Vec<String>,
    /// Symbols that exist and are unchanged (skip database write)
    pub unchanged: Vec<SymbolInput>,
}

/// Compare old symbols (from database) against new symbols (just extracted)
///
/// This function uses stable IDs to match symbols and identify what changed:
/// - **Added:** Symbols in new but not in old
/// - **Modified:** Symbols in both but content changed (line numbers, docstring)
/// - **Deleted:** Symbols in old but not in new
/// - **Unchanged:** Symbols in both with identical content (skip db write)
///
/// # Arguments
///
/// * `old_symbols` - Existing symbols from database for this file
/// * `new_symbols` - Newly extracted symbols from parsing the file
///
/// # Returns
///
/// A SymbolDiff indicating what changed
///
/// # Algorithm
///
/// 1. Build hashmaps keyed by stable_id for fast lookup
/// 2. For each new symbol:
///    - If stable_id not in old → added
///    - If stable_id in old but content changed → modified
///    - If stable_id in old and content same → unchanged
/// 3. For each old symbol:
///    - If stable_id not in new → deleted
pub fn compute_symbol_diff(old_symbols: Vec<Symbol>, new_symbols: Vec<SymbolInput>) -> SymbolDiff {
    // Build map of old symbols keyed by stable_id
    let old_map: HashMap<String, Symbol> = old_symbols
        .into_iter()
        .filter_map(|s| {
            // Skip symbols without stable_id (shouldn't happen after Phase 1, but be safe)
            s.stable_id.clone().map(|stable_id| (stable_id, s))
        })
        .collect();

    // Build map of new symbols keyed by computed stable_id
    let new_map: HashMap<String, SymbolInput> = new_symbols
        .into_iter()
        .map(|s| {
            let stable_id = compute_stable_symbol_id(&s);
            (stable_id, s)
        })
        .collect();

    let mut diff = SymbolDiff::default();

    // Find added, modified, and unchanged symbols
    for (stable_id, new_sym) in &new_map {
        match old_map.get(stable_id) {
            None => {
                // Symbol is new
                diff.added.push(new_sym.clone());
            }
            Some(old_sym) => {
                // Symbol exists - check if content changed
                if symbol_content_changed(old_sym, new_sym) {
                    diff.modified.push(new_sym.clone());
                } else {
                    diff.unchanged.push(new_sym.clone());
                }
            }
        }
    }

    // Find deleted symbols
    for (stable_id, _) in &old_map {
        if !new_map.contains_key(stable_id) {
            diff.deleted.push(stable_id.clone());
        }
    }

    diff
}

/// Check if symbol content changed (fields that can change without changing stable_id)
///
/// Stable ID includes: qualname, signature, kind
/// These fields can change without affecting stable_id:
/// - start_line, end_line (code moved)
/// - start_col, end_col (indentation changed)
/// - start_byte, end_byte (derived from lines)
/// - docstring (documentation updated)
///
/// Note: If signature changes, stable_id changes, so they won't match in the first place.
fn symbol_content_changed(old: &Symbol, new: &SymbolInput) -> bool {
    // Compare mutable fields
    old.start_line != new.start_line
        || old.end_line != new.end_line
        || old.start_col != new.start_col
        || old.end_col != new.end_col
        || old.start_byte != new.start_byte
        || old.end_byte != new.end_byte
        || old.docstring != new.docstring
    // signature is already in stable_id, so changes there → different stable_id → added/deleted
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_old_symbol(
        qualname: &str,
        start_line: i64,
        end_line: i64,
        start_byte: i64,
        end_byte: i64,
        docstring: Option<&str>,
        signature: Option<&str>,
    ) -> Symbol {
        let sig = signature
            .map(String::from)
            .or_else(|| Some(format!("def {}():", qualname)));

        // Compute stable_id from the symbol input to ensure consistency
        let input = SymbolInput {
            kind: "function".to_string(),
            name: qualname.split('.').last().unwrap().to_string(),
            qualname: qualname.to_string(),
            start_line,
            start_col: 0,
            end_line,
            end_col: 0,
            start_byte,
            end_byte,
            signature: sig.clone(),
            docstring: docstring.map(String::from),
        };
        let stable_id = compute_stable_symbol_id(&input);

        Symbol {
            id: 1,
            file_path: "test.py".to_string(),
            kind: "function".to_string(),
            name: qualname.split('.').last().unwrap().to_string(),
            qualname: qualname.to_string(),
            start_line,
            start_col: 0,
            end_line,
            end_col: 0,
            start_byte,
            end_byte,
            signature: sig,
            docstring: docstring.map(String::from),
            graph_version: 1,
            commit_sha: None,
            stable_id: Some(stable_id),
        }
    }

    fn make_new_symbol(
        qualname: &str,
        start_line: i64,
        end_line: i64,
        start_byte: i64,
        end_byte: i64,
        docstring: Option<&str>,
        signature: Option<&str>,
    ) -> SymbolInput {
        let sig = signature
            .map(String::from)
            .or_else(|| Some(format!("def {}():", qualname)));

        SymbolInput {
            kind: "function".to_string(),
            name: qualname.split('.').last().unwrap().to_string(),
            qualname: qualname.to_string(),
            start_line,
            start_col: 0,
            end_line,
            end_col: 0,
            start_byte,
            end_byte,
            signature: sig,
            docstring: docstring.map(String::from),
        }
    }

    #[test]
    fn test_all_symbols_added() {
        let old_symbols = vec![];
        let new_symbols = vec![
            make_new_symbol("test.foo", 1, 5, 0, 100, None, None),
            make_new_symbol("test.bar", 7, 12, 200, 300, None, None),
        ];

        let diff = compute_symbol_diff(old_symbols, new_symbols);

        assert_eq!(diff.added.len(), 2);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.deleted.len(), 0);
        assert_eq!(diff.unchanged.len(), 0);
    }

    #[test]
    fn test_all_symbols_deleted() {
        let old_symbols = vec![
            make_old_symbol("test.foo", 1, 5, 0, 100, None, None),
            make_old_symbol("test.bar", 7, 12, 200, 300, None, None),
        ];
        let new_symbols = vec![];

        let diff = compute_symbol_diff(old_symbols, new_symbols);

        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.deleted.len(), 2);
        assert_eq!(diff.unchanged.len(), 0);
        // Don't check exact stable_id values since they're computed
        assert!(diff.deleted.len() == 2);
    }

    #[test]
    fn test_symbol_modified_line_change() {
        // Symbol moved down (blank line added above)
        let old = make_old_symbol("test.foo", 10, 15, 100, 200, None, None);
        let new = make_new_symbol("test.foo", 11, 16, 110, 210, None, None); // Moved down 1 line

        let old_symbols = vec![old];
        let new_symbols = vec![new];

        let diff = compute_symbol_diff(old_symbols, new_symbols);

        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.deleted.len(), 0);
        assert_eq!(diff.unchanged.len(), 0);
        assert_eq!(diff.modified[0].qualname, "test.foo");
    }

    #[test]
    fn test_symbol_modified_docstring_change() {
        let old = make_old_symbol("test.foo", 10, 15, 100, 200, Some("Old docstring"), None);
        let new = make_new_symbol("test.foo", 10, 15, 100, 200, Some("New docstring"), None);

        let old_symbols = vec![old];
        let new_symbols = vec![new];

        let diff = compute_symbol_diff(old_symbols, new_symbols);

        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.deleted.len(), 0);
        assert_eq!(diff.unchanged.len(), 0);
    }

    #[test]
    fn test_symbol_unchanged() {
        let old = make_old_symbol("test.foo", 10, 15, 100, 200, Some("Doc"), None);
        let new = make_new_symbol("test.foo", 10, 15, 100, 200, Some("Doc"), None);

        let old_symbols = vec![old];
        let new_symbols = vec![new];

        let diff = compute_symbol_diff(old_symbols, new_symbols);

        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.deleted.len(), 0);
        assert_eq!(diff.unchanged.len(), 1);
        assert_eq!(diff.unchanged[0].qualname, "test.foo");
    }

    #[test]
    fn test_mixed_changes() {
        // Old: foo (unchanged), bar (modified), baz (deleted)
        // New: foo (unchanged), bar (modified), qux (added)
        let old_symbols = vec![
            make_old_symbol("test.foo", 1, 5, 0, 100, None, None),
            make_old_symbol("test.bar", 7, 12, 200, 300, Some("Old doc"), None),
            make_old_symbol("test.baz", 14, 20, 400, 500, None, None),
        ];
        let new_symbols = vec![
            make_new_symbol("test.foo", 1, 5, 0, 100, None, None), // unchanged
            make_new_symbol("test.bar", 7, 12, 200, 300, Some("New doc"), None), // modified (docstring)
            make_new_symbol("test.qux", 22, 28, 600, 700, None, None),           // added
        ];

        let diff = compute_symbol_diff(old_symbols, new_symbols);

        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.deleted.len(), 1);
        assert_eq!(diff.unchanged.len(), 1);

        assert_eq!(diff.added[0].qualname, "test.qux");
        assert_eq!(diff.modified[0].qualname, "test.bar");
        assert!(diff.deleted.len() == 1); // baz was deleted
        assert_eq!(diff.unchanged[0].qualname, "test.foo");
    }

    #[test]
    fn test_signature_change_creates_different_stable_id() {
        // When signature changes, stable_id changes
        // So old symbol is "deleted" and new symbol is "added"
        let old = make_old_symbol("test.foo", 10, 15, 100, 200, None, Some("def foo():"));

        // New symbol with different signature (stable_id will be different)
        let new = make_new_symbol("test.foo", 10, 15, 100, 200, None, Some("def foo(x: int):"));

        let old_symbols = vec![old];
        let new_symbols = vec![new];

        let diff = compute_symbol_diff(old_symbols, new_symbols);

        // Signature change → different stable_id → appears as delete + add
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.deleted.len(), 1);
        assert_eq!(diff.unchanged.len(), 0);
    }

    #[test]
    fn test_empty_file() {
        let old_symbols = vec![];
        let new_symbols = vec![];

        let diff = compute_symbol_diff(old_symbols, new_symbols);

        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.deleted.len(), 0);
        assert_eq!(diff.unchanged.len(), 0);
    }

    #[test]
    fn test_all_symbols_changed() {
        // Every symbol is different
        let old_symbols = vec![
            make_old_symbol("test.foo", 1, 5, 0, 100, None, None),
            make_old_symbol("test.bar", 7, 12, 200, 300, None, None),
        ];
        let new_symbols = vec![
            make_new_symbol("test.baz", 1, 5, 0, 100, None, None),
            make_new_symbol("test.qux", 7, 12, 200, 300, None, None),
        ];

        let diff = compute_symbol_diff(old_symbols, new_symbols);

        assert_eq!(diff.added.len(), 2);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.deleted.len(), 2);
        assert_eq!(diff.unchanged.len(), 0);
    }

    #[test]
    fn test_byte_offset_change_detected() {
        let old = make_old_symbol("test.foo", 10, 15, 100, 200, None, None);
        let new = make_new_symbol("test.foo", 10, 15, 150, 250, None, None); // Changed byte offset

        let old_symbols = vec![old];
        let new_symbols = vec![new];

        let diff = compute_symbol_diff(old_symbols, new_symbols);

        // Byte offset change should trigger modification
        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.deleted.len(), 0);
        assert_eq!(diff.unchanged.len(), 0);
    }
}

// Integration tests (require database)
#[cfg(test)]
mod integration_tests {
    use super::*;
    use crate::db::Db;
    use crate::indexer::extract::SymbolInput;
    use tempfile::TempDir;

    fn setup_test_db() -> (TempDir, Db) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Db::new(&db_path).unwrap();
        (temp_dir, db)
    }

    fn make_symbol_input(qualname: &str, line: i64, sig: &str) -> SymbolInput {
        SymbolInput {
            kind: "function".to_string(),
            name: qualname.split('.').last().unwrap().to_string(),
            qualname: qualname.to_string(),
            start_line: line,
            start_col: 0,
            end_line: line + 5,
            end_col: 0,
            start_byte: line * 100,
            end_byte: (line + 5) * 100,
            signature: Some(sig.to_string()),
            docstring: None,
        }
    }

    #[test]
    fn test_integration_reindex_detects_added_symbol() {
        let (_temp_dir, mut db) = setup_test_db();
        let graph_version = db.current_graph_version().unwrap();

        // Index a file with one symbol
        let file_id = db
            .upsert_file("test.py", "hash1", "python", 100, 12345)
            .unwrap();
        let symbols = vec![make_symbol_input("test.foo", 1, "def foo():")];
        db.insert_symbols(file_id, "test.py", &symbols, graph_version, None)
            .unwrap();

        // Fetch existing symbols
        let existing = db.get_symbols_for_file("test.py", graph_version).unwrap();
        assert_eq!(existing.len(), 1);

        // Create new symbols list with added symbol
        let new_symbols = vec![
            make_symbol_input("test.foo", 1, "def foo():"),
            make_symbol_input("test.bar", 10, "def bar():"), // Added
        ];

        // Compute diff
        let diff = compute_symbol_diff(existing, new_symbols);

        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.deleted.len(), 0);
        assert_eq!(diff.unchanged.len(), 1);
        assert_eq!(diff.added[0].qualname, "test.bar");
    }

    #[test]
    fn test_integration_reindex_detects_modified_symbol() {
        let (_temp_dir, mut db) = setup_test_db();
        let graph_version = db.current_graph_version().unwrap();

        // Index a file with one symbol
        let file_id = db
            .upsert_file("test.py", "hash1", "python", 100, 12345)
            .unwrap();
        let symbols = vec![make_symbol_input("test.foo", 1, "def foo():")];
        db.insert_symbols(file_id, "test.py", &symbols, graph_version, None)
            .unwrap();

        // Fetch existing symbols
        let existing = db.get_symbols_for_file("test.py", graph_version).unwrap();
        assert_eq!(existing.len(), 1);

        // Create new symbols list with modified symbol (line changed)
        let new_symbols = vec![
            make_symbol_input("test.foo", 5, "def foo():"), // Line changed
        ];

        // Compute diff
        let diff = compute_symbol_diff(existing, new_symbols);

        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.deleted.len(), 0);
        assert_eq!(diff.unchanged.len(), 0);
        assert_eq!(diff.modified[0].qualname, "test.foo");
    }

    #[test]
    fn test_integration_reindex_detects_deleted_symbol() {
        let (_temp_dir, mut db) = setup_test_db();
        let graph_version = db.current_graph_version().unwrap();

        // Index a file with two symbols
        let file_id = db
            .upsert_file("test.py", "hash1", "python", 100, 12345)
            .unwrap();
        let symbols = vec![
            make_symbol_input("test.foo", 1, "def foo():"),
            make_symbol_input("test.bar", 10, "def bar():"),
        ];
        db.insert_symbols(file_id, "test.py", &symbols, graph_version, None)
            .unwrap();

        // Fetch existing symbols
        let existing = db.get_symbols_for_file("test.py", graph_version).unwrap();
        assert_eq!(existing.len(), 2);

        // Create new symbols list with one symbol deleted
        let new_symbols = vec![
            make_symbol_input("test.foo", 1, "def foo():"), // bar is deleted
        ];

        // Compute diff
        let diff = compute_symbol_diff(existing, new_symbols);

        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.deleted.len(), 1);
        assert_eq!(diff.unchanged.len(), 1);
    }

    #[test]
    fn test_integration_reindex_complex_changes() {
        let (_temp_dir, mut db) = setup_test_db();
        let graph_version = db.current_graph_version().unwrap();

        // Index a file with three symbols
        let file_id = db
            .upsert_file("test.py", "hash1", "python", 100, 12345)
            .unwrap();
        let symbols = vec![
            make_symbol_input("test.foo", 1, "def foo():"),
            make_symbol_input("test.bar", 10, "def bar():"),
            make_symbol_input("test.baz", 20, "def baz():"),
        ];
        db.insert_symbols(file_id, "test.py", &symbols, graph_version, None)
            .unwrap();

        // Fetch existing symbols
        let existing = db.get_symbols_for_file("test.py", graph_version).unwrap();
        assert_eq!(existing.len(), 3);

        // Create new symbols list:
        // - foo: unchanged
        // - bar: modified (line changed)
        // - baz: deleted
        // - qux: added
        let new_symbols = vec![
            make_symbol_input("test.foo", 1, "def foo():"), // unchanged
            make_symbol_input("test.bar", 15, "def bar():"), // modified (line)
            make_symbol_input("test.qux", 30, "def qux():"), // added
        ];

        // Compute diff
        let diff = compute_symbol_diff(existing, new_symbols);

        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.deleted.len(), 1);
        assert_eq!(diff.unchanged.len(), 1);
        assert_eq!(diff.added[0].qualname, "test.qux");
        assert_eq!(diff.modified[0].qualname, "test.bar");
        assert_eq!(diff.unchanged[0].qualname, "test.foo");
    }

    // ===== Phase 6: Edge Case Tests =====

    #[test]
    fn test_edge_case_empty_file() {
        // Test: File with no symbols (empty file or all comments)
        let (_temp_dir, mut db) = setup_test_db();
        let graph_version = db.current_graph_version().unwrap();

        // Index a file with symbols
        let file_id = db
            .upsert_file("test.py", "hash1", "python", 100, 12345)
            .unwrap();
        let symbols = vec![make_symbol_input("test.foo", 1, "def foo():")];
        db.insert_symbols(file_id, "test.py", &symbols, graph_version, None)
            .unwrap();

        let existing = db.get_symbols_for_file("test.py", graph_version).unwrap();
        assert_eq!(existing.len(), 1);

        // File now empty (no symbols)
        let new_symbols = vec![];

        let diff = compute_symbol_diff(existing, new_symbols);

        // All symbols should be deleted
        assert_eq!(diff.added.len(), 0);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.deleted.len(), 1);
        assert_eq!(diff.unchanged.len(), 0);
    }

    #[test]
    fn test_edge_case_all_symbols_added() {
        // Test: New file with symbols (previously empty or didn't exist)
        let (_temp_dir, mut db) = setup_test_db();
        let graph_version = db.current_graph_version().unwrap();

        // Index empty file
        let file_id = db
            .upsert_file("test.py", "hash1", "python", 100, 12345)
            .unwrap();
        let symbols = vec![];
        db.insert_symbols(file_id, "test.py", &symbols, graph_version, None)
            .unwrap();

        let existing = db.get_symbols_for_file("test.py", graph_version).unwrap();
        assert_eq!(existing.len(), 0);

        // File now has symbols
        let new_symbols = vec![
            make_symbol_input("test.foo", 1, "def foo():"),
            make_symbol_input("test.bar", 10, "def bar():"),
        ];

        let diff = compute_symbol_diff(existing, new_symbols);

        // All symbols should be added
        assert_eq!(diff.added.len(), 2);
        assert_eq!(diff.modified.len(), 0);
        assert_eq!(diff.deleted.len(), 0);
        assert_eq!(diff.unchanged.len(), 0);
    }

    #[test]
    fn test_edge_case_very_long_signature() {
        // Test: Symbol with very long signature (>1000 chars)
        let long_sig = "a".repeat(2000);

        let symbols1 = vec![SymbolInput {
            kind: "function".to_string(),
            name: "foo".to_string(),
            qualname: "test.foo".to_string(),
            signature: Some(long_sig.clone()),
            start_line: 1,
            start_col: 0,
            end_line: 10,
            end_col: 0,
            start_byte: 0,
            end_byte: 1000,
            docstring: None,
        }];

        let symbols2 = vec![SymbolInput {
            kind: "function".to_string(),
            name: "foo".to_string(),
            qualname: "test.foo".to_string(),
            signature: Some(long_sig),
            start_line: 1,
            start_col: 0,
            end_line: 10,
            end_col: 0,
            start_byte: 0,
            end_byte: 1000,
            docstring: None,
        }];

        let diff = compute_symbol_diff(vec![], symbols1);
        assert_eq!(diff.added.len(), 1);

        // Recompute with same signature - should be unchanged
        let diff2 = compute_symbol_diff(
            vec![Symbol {
                id: 1,
                stable_id: Some(compute_stable_symbol_id(&diff.added[0])),
                file_path: "test.py".to_string(),
                kind: diff.added[0].kind.clone(),
                name: diff.added[0].name.clone(),
                qualname: diff.added[0].qualname.clone(),
                signature: diff.added[0].signature.clone(),
                start_line: diff.added[0].start_line,
                start_col: diff.added[0].start_col,
                end_line: diff.added[0].end_line,
                end_col: diff.added[0].end_col,
                start_byte: diff.added[0].start_byte,
                end_byte: diff.added[0].end_byte,
                docstring: diff.added[0].docstring.clone(),
                graph_version: 1,
                commit_sha: None,
            }],
            symbols2,
        );
        assert_eq!(diff2.unchanged.len(), 1);
    }

    #[test]
    fn test_edge_case_duplicate_qualnames() {
        // Test: Multiple symbols with same qualname (e.g., overloaded functions)
        // In practice this shouldn't happen, but test graceful handling
        let symbols = vec![
            SymbolInput {
                kind: "function".to_string(),
                name: "foo".to_string(),
                qualname: "test.foo".to_string(),
                signature: Some("(int x)".to_string()),
                start_line: 1,
                start_col: 0,
                end_line: 5,
                end_col: 0,
                start_byte: 0,
                end_byte: 500,
                docstring: None,
            },
            SymbolInput {
                kind: "function".to_string(),
                name: "foo".to_string(),
                qualname: "test.foo".to_string(),
                signature: Some("(str x)".to_string()), // Different signature
                start_line: 10,
                start_col: 0,
                end_line: 15,
                end_col: 0,
                start_byte: 1000,
                end_byte: 1500,
                docstring: None,
            },
        ];

        let diff = compute_symbol_diff(vec![], symbols);

        // Should add both (different stable_ids due to different signatures)
        assert_eq!(diff.added.len(), 2);

        // Verify they have different stable IDs
        let id1 = compute_stable_symbol_id(&diff.added[0]);
        let id2 = compute_stable_symbol_id(&diff.added[1]);
        assert_ne!(
            id1, id2,
            "Different signatures should produce different stable IDs"
        );
    }

    #[test]
    fn test_edge_case_many_symbols() {
        // Test: File with 1000+ symbols
        let mut symbols = Vec::new();
        for i in 0..1500 {
            symbols.push(make_symbol_input(
                &format!("test.func{}", i),
                i as i64 * 10,
                &format!("def func{}():", i),
            ));
        }

        let diff = compute_symbol_diff(vec![], symbols.clone());
        assert_eq!(diff.added.len(), 1500);

        // Now modify half of them
        let mut modified_symbols = symbols.clone();
        for i in 0..750 {
            modified_symbols[i].start_line += 1; // Change line
        }

        // Convert to existing symbols
        let existing: Vec<Symbol> = diff
            .added
            .iter()
            .map(|s| Symbol {
                id: 1,
                stable_id: Some(compute_stable_symbol_id(s)),
                file_path: "test.py".to_string(),
                kind: s.kind.clone(),
                name: s.name.clone(),
                qualname: s.qualname.clone(),
                signature: s.signature.clone(),
                start_line: s.start_line,
                start_col: s.start_col,
                end_line: s.end_line,
                end_col: s.end_col,
                start_byte: s.start_byte,
                end_byte: s.end_byte,
                docstring: s.docstring.clone(),
                graph_version: 1,
                commit_sha: None,
            })
            .collect();

        let diff2 = compute_symbol_diff(existing, modified_symbols);
        assert_eq!(diff2.modified.len(), 750);
        assert_eq!(diff2.unchanged.len(), 750);
    }

    #[test]
    fn test_edge_case_symbol_at_boundaries() {
        // Test: Symbol at line 0 and very high line numbers
        let symbols = vec![
            make_symbol_input("test.start", 0, "# Module docstring"),
            make_symbol_input("test.end", 999999, "def end():"),
        ];

        let diff = compute_symbol_diff(vec![], symbols);
        assert_eq!(diff.added.len(), 2);
    }

    #[test]
    fn test_edge_case_empty_qualname() {
        // Test: Symbol with empty qualname (shouldn't happen but test gracefully)
        let symbols = vec![SymbolInput {
            kind: "unknown".to_string(),
            name: "".to_string(),
            qualname: "".to_string(),
            signature: None,
            start_line: 1,
            start_col: 0,
            end_line: 1,
            end_col: 0,
            start_byte: 0,
            end_byte: 10,
            docstring: None,
        }];

        let diff = compute_symbol_diff(vec![], symbols);

        // Should still work, just with empty qualname
        assert_eq!(diff.added.len(), 1);

        // Stable ID should still be computed
        let stable_id = compute_stable_symbol_id(&diff.added[0]);
        assert!(!stable_id.is_empty());
    }

    #[test]
    fn test_edge_case_special_characters_in_qualname() {
        // Test: Symbols with Unicode, emoji, special characters
        let symbols = vec![
            make_symbol_input("test.函数", 1, "def 函数():"),
            make_symbol_input("test.función", 10, "def función():"),
            make_symbol_input("test.🚀_rocket", 20, "def 🚀_rocket():"),
            make_symbol_input("test.with spaces", 30, "# Invalid but test it"),
        ];

        let diff = compute_symbol_diff(vec![], symbols);
        assert_eq!(diff.added.len(), 4);

        // All should have unique stable IDs
        let ids: Vec<String> = diff
            .added
            .iter()
            .map(|s| compute_stable_symbol_id(s))
            .collect();

        let unique_ids: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique_ids.len(), 4, "All symbols should have unique IDs");
    }

    #[test]
    fn test_edge_case_docstring_changes_only() {
        // Test: Only docstring changes (should be detected as modified)
        let (_temp_dir, mut db) = setup_test_db();
        let graph_version = db.current_graph_version().unwrap();

        let file_id = db
            .upsert_file("test.py", "hash1", "python", 100, 12345)
            .unwrap();
        let symbols = vec![SymbolInput {
            kind: "function".to_string(),
            name: "foo".to_string(),
            qualname: "test.foo".to_string(),
            signature: Some("()".to_string()),
            start_line: 1,
            start_col: 0,
            end_line: 5,
            end_col: 0,
            start_byte: 0,
            end_byte: 500,
            docstring: Some("Old docstring".to_string()),
        }];
        db.insert_symbols(file_id, "test.py", &symbols, graph_version, None)
            .unwrap();

        let existing = db.get_symbols_for_file("test.py", graph_version).unwrap();

        // Same symbol but different docstring
        let new_symbols = vec![SymbolInput {
            kind: "function".to_string(),
            name: "foo".to_string(),
            qualname: "test.foo".to_string(),
            signature: Some("()".to_string()),
            start_line: 1,
            start_col: 0,
            end_line: 5,
            end_col: 0,
            start_byte: 0,
            end_byte: 500,
            docstring: Some("New docstring".to_string()),
        }];

        let diff = compute_symbol_diff(existing, new_symbols);

        // Should be detected as modified
        assert_eq!(diff.modified.len(), 1);
        assert_eq!(diff.unchanged.len(), 0);
    }
}
