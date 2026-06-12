use crate::config::Config;
use crate::indexer::channel::is_bridge_edge_kind;
use crate::indexer::differ::SymbolDiff;
use crate::indexer::extract::{EdgeInput, SymbolInput};
use crate::metrics::{FileMetricsInput, SymbolMetricsInput};
use crate::model::{Edge, GraphVersion, Symbol};
use anyhow::{Context, Result};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OptionalExtension, Row, params};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod analytics;
mod co_change;
mod graph_query;
mod migrations;
mod overview;

#[derive(Debug, Clone)]
pub struct ModuleSummaryEntry {
    pub path: String,
    pub file_count: usize,
    pub symbol_count: usize,
    pub languages: Vec<String>,
}

#[derive(Debug)]
struct ConnectionCustomizer;

impl r2d2::CustomizeConnection<Connection, rusqlite::Error> for ConnectionCustomizer {
    fn on_acquire(&self, conn: &mut Connection) -> Result<(), rusqlite::Error> {
        conn.busy_timeout(Duration::from_secs(30))?;
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;
            ",
        )?;

        Ok(())
    }

    fn on_release(&self, _conn: Connection) {}
}

#[derive(Debug, Clone)]
pub struct FileRecord {
    pub id: i64,
    pub path: String,
    pub hash: String,
    pub language: String,
    pub deleted_version: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SymbolRefRecord {
    pub id: i64,
    pub name: String,
    pub qualname: String,
    pub kind: String,
    pub language: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDigest {
    pub rows: usize,
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbDigest {
    pub files: TableDigest,
    pub symbols: TableDigest,
    pub edges: TableDigest,
}

pub struct Db {
    db_path: PathBuf,
    write_conn: Arc<Mutex<Connection>>,
    read_pool: Pool<SqliteConnectionManager>,
}

impl Db {
    pub fn new(db_path: &Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create db directory {}", parent.display()))?;
        }

        // Get configuration
        let config = Config::get();
        eprintln!(
            "lidx: Initializing connection pool (size: {}, min_idle: {})",
            config.pool_size, config.pool_min_idle
        );

        // Open write connection first and run migrations
        let write_conn = Connection::open(db_path)
            .with_context(|| format!("open sqlite db at {}", db_path.display()))?;
        write_conn.busy_timeout(Duration::from_secs(30))?;
        write_conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;
            ",
        )?;
        migrations::migrate(&write_conn)?;

        // Wrap write connection in Arc<Mutex<>>
        let write_conn = Arc::new(Mutex::new(write_conn));

        // Create read pool
        let manager = SqliteConnectionManager::file(db_path);
        let read_pool = Pool::builder()
            .max_size(config.pool_size)
            .min_idle(Some(config.pool_min_idle))
            .connection_timeout(Duration::from_secs(30))
            .connection_customizer(Box::new(ConnectionCustomizer))
            .build(manager)
            .with_context(|| "create connection pool")?;

        eprintln!("lidx: Database connection pool initialized");

        Ok(Self {
            db_path: db_path.to_path_buf(),
            write_conn,
            read_pool,
        })
    }

    /// Get the database file path
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn read_conn(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        self.read_pool
            .get()
            .with_context(|| "get read connection from pool")
    }

    fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.write_conn.lock().unwrap()
    }

    pub fn list_files(&self, graph_version: i64) -> Result<Vec<FileRecord>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, path, hash, language, deleted_version
             FROM files
             WHERE deleted_version IS NULL OR deleted_version > ?
             ORDER BY path",
        )?;
        let rows = stmt.query_map(params![graph_version], |row| {
            Ok(FileRecord {
                id: row.get(0)?,
                path: row.get(1)?,
                hash: row.get(2)?,
                language: row.get(3)?,
                deleted_version: row.get(4)?,
            })
        })?;

        let mut records = Vec::new();
        for row in rows {
            records.push(row?);
        }
        Ok(records)
    }

    pub fn get_file_by_path(&self, path: &str) -> Result<Option<FileRecord>> {
        self.read_conn()?
            .query_row(
                "SELECT id, path, hash, language, deleted_version FROM files WHERE path = ?",
                params![path],
                |row| {
                    Ok(FileRecord {
                        id: row.get(0)?,
                        path: row.get(1)?,
                        hash: row.get(2)?,
                        language: row.get(3)?,
                        deleted_version: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn upsert_file(
        &self,
        path: &str,
        hash: &str,
        language: &str,
        size: i64,
        modified: i64,
    ) -> Result<i64> {
        let conn = self.conn();
        conn.execute(
            "INSERT INTO files (path, hash, language, size, modified, deleted_version)
             VALUES (?, ?, ?, ?, ?, NULL)
             ON CONFLICT(path) DO UPDATE SET
                hash = excluded.hash,
                language = excluded.language,
                size = excluded.size,
                modified = excluded.modified,
                deleted_version = NULL",
            params![path, hash, language, size, modified],
        )?;
        let id: i64 = conn.query_row(
            "SELECT id FROM files WHERE path = ?",
            params![path],
            |row| row.get(0),
        )?;
        Ok(id)
    }

    pub fn delete_file_by_path(&self, path: &str) -> Result<()> {
        self.conn()
            .execute("DELETE FROM files WHERE path = ?", params![path])?;
        Ok(())
    }

    pub fn mark_file_deleted(&self, path: &str, graph_version: i64) -> Result<()> {
        self.conn().execute(
            "UPDATE files
             SET deleted_version = CASE
                WHEN deleted_version IS NULL OR deleted_version > ? THEN ?
                ELSE deleted_version
             END
             WHERE path = ?",
            params![graph_version, graph_version, path],
        )?;
        Ok(())
    }

    pub fn delete_edges_by_kind(&self, kind: &str, graph_version: i64) -> Result<()> {
        self.conn().execute(
            "DELETE FROM edges WHERE kind = ? AND graph_version = ?",
            params![kind, graph_version],
        )?;
        Ok(())
    }

    /// Delete edges for a file (helper for incremental updates)
    pub fn delete_edges_for_file(&self, file_id: i64, graph_version: i64) -> Result<()> {
        self.conn().execute(
            "DELETE FROM edges WHERE file_id = ? AND graph_version = ?",
            params![file_id, graph_version],
        )?;
        Ok(())
    }

    /// Delete all symbols, edges, and metrics for a file (legacy method)
    ///
    /// Note: This is the old approach. For incremental updates, prefer:
    /// - `update_file_symbols()` for symbols (Phase 3)
    /// - `delete_edges_for_file()` + `insert_edges()` for edges
    pub fn delete_symbols_edges_for_file(&self, file_id: i64, graph_version: i64) -> Result<()> {
        self.conn().execute(
            "DELETE FROM edges WHERE file_id = ? AND graph_version = ?",
            params![file_id, graph_version],
        )?;
        self.conn().execute(
            "DELETE FROM symbols WHERE file_id = ? AND graph_version = ?",
            params![file_id, graph_version],
        )?;
        self.conn().execute(
            "DELETE FROM file_metrics WHERE file_id = ?",
            params![file_id],
        )?;
        Ok(())
    }

    pub fn insert_symbols(
        &mut self,
        file_id: i64,
        file_path: &str,
        symbols: &[SymbolInput],
        graph_version: i64,
        commit_sha: Option<&str>,
    ) -> Result<Vec<Symbol>> {
        use crate::indexer::stable_id::compute_stable_symbol_id;

        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut inserted = Vec::with_capacity(symbols.len());
        {
            let mut stmt = tx.prepare(
                "INSERT INTO symbols
                 (file_id, kind, name, qualname, start_line, start_col, end_line, end_col, start_byte, end_byte, signature, docstring, graph_version, commit_sha, stable_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            for symbol in symbols {
                // Compute stable ID for this symbol
                let stable_id = compute_stable_symbol_id(symbol);

                stmt.execute(params![
                    file_id,
                    &symbol.kind,
                    &symbol.name,
                    &symbol.qualname,
                    symbol.start_line,
                    symbol.start_col,
                    symbol.end_line,
                    symbol.end_col,
                    symbol.start_byte,
                    symbol.end_byte,
                    symbol.signature.as_deref(),
                    symbol.docstring.as_deref(),
                    graph_version,
                    commit_sha,
                    &stable_id,
                ])?;
                let id = tx.last_insert_rowid();
                inserted.push(Symbol {
                    id,
                    file_path: file_path.to_string(),
                    kind: symbol.kind.clone(),
                    name: symbol.name.clone(),
                    qualname: symbol.qualname.clone(),
                    start_line: symbol.start_line,
                    start_col: symbol.start_col,
                    end_line: symbol.end_line,
                    end_col: symbol.end_col,
                    start_byte: symbol.start_byte,
                    end_byte: symbol.end_byte,
                    signature: symbol.signature.clone(),
                    docstring: symbol.docstring.clone(),
                    graph_version,
                    commit_sha: commit_sha.map(str::to_string),
                    stable_id: Some(stable_id),
                });
            }
        }
        tx.commit()?;
        Ok(inserted)
    }

    /// Update file symbols using incremental diff (Phase 3)
    ///
    /// This method uses a SymbolDiff to perform smart database updates:
    /// - DELETE only removed symbols (by stable_id)
    /// - INSERT new symbols
    /// - UPDATE modified symbols (by stable_id)
    /// - SKIP unchanged symbols entirely
    ///
    /// This is much more efficient than the old delete-all-then-insert approach,
    /// especially for small changes where most symbols are unchanged.
    ///
    /// # Arguments
    ///
    /// * `file_id` - The database ID of the file
    /// * `file_path` - The file path (for constructing Symbol objects)
    /// * `diff` - The SymbolDiff containing added/modified/deleted/unchanged symbols
    /// * `graph_version` - The current graph version
    /// * `commit_sha` - Optional git commit SHA
    ///
    /// # Returns
    ///
    /// A vector of all symbols for the file (needed for edge resolution)
    ///
    /// # Performance
    ///
    /// For a file with 100 symbols where 1 changed:
    /// - Old approach: 1 DELETE + 100 INSERT = 101 operations
    /// - New approach: 1 UPDATE = 1 operation (100x improvement!)
    pub fn update_file_symbols(
        &mut self,
        file_id: i64,
        file_path: &str,
        diff: SymbolDiff,
        graph_version: i64,
        commit_sha: Option<&str>,
    ) -> Result<Vec<Symbol>> {
        use crate::indexer::stable_id::compute_stable_symbol_id;

        let mut conn = self.conn();
        let tx = conn.transaction()?;

        // Track all symbols for return (needed for edge resolution)
        let mut all_symbols =
            Vec::with_capacity(diff.added.len() + diff.modified.len() + diff.unchanged.len());

        // PHASE 1: DELETE removed symbols (by stable_id)
        if !diff.deleted.is_empty() {
            let placeholders = vec!["?"; diff.deleted.len()].join(",");
            let delete_sql = format!(
                "DELETE FROM symbols WHERE stable_id IN ({}) AND graph_version = ?",
                placeholders
            );

            // Build params: all stable_ids + graph_version at the end
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
            for stable_id in &diff.deleted {
                params.push(Box::new(stable_id.clone()));
            }
            params.push(Box::new(graph_version));

            tx.execute(
                &delete_sql,
                rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
            )?;
        }

        // PHASE 2: INSERT new symbols
        if !diff.added.is_empty() {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO symbols
                 (file_id, kind, name, qualname, start_line, start_col, end_line, end_col, start_byte, end_byte, signature, docstring, graph_version, commit_sha, stable_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )?;

            for symbol in &diff.added {
                let stable_id = compute_stable_symbol_id(symbol);

                stmt.execute(params![
                    file_id,
                    &symbol.kind,
                    &symbol.name,
                    &symbol.qualname,
                    symbol.start_line,
                    symbol.start_col,
                    symbol.end_line,
                    symbol.end_col,
                    symbol.start_byte,
                    symbol.end_byte,
                    symbol.signature.as_deref(),
                    symbol.docstring.as_deref(),
                    graph_version,
                    commit_sha,
                    &stable_id,
                ])?;

                let id = tx.last_insert_rowid();
                all_symbols.push(Symbol {
                    id,
                    file_path: file_path.to_string(),
                    kind: symbol.kind.clone(),
                    name: symbol.name.clone(),
                    qualname: symbol.qualname.clone(),
                    start_line: symbol.start_line,
                    start_col: symbol.start_col,
                    end_line: symbol.end_line,
                    end_col: symbol.end_col,
                    start_byte: symbol.start_byte,
                    end_byte: symbol.end_byte,
                    signature: symbol.signature.clone(),
                    docstring: symbol.docstring.clone(),
                    graph_version,
                    commit_sha: commit_sha.map(str::to_string),
                    stable_id: Some(stable_id),
                });
            }
        }

        // PHASE 3: UPDATE modified symbols (by stable_id)
        if !diff.modified.is_empty() {
            let mut stmt = tx.prepare_cached(
                "UPDATE symbols
                 SET start_line = ?, start_col = ?, end_line = ?, end_col = ?,
                     start_byte = ?, end_byte = ?, docstring = ?
                 WHERE stable_id = ? AND graph_version = ?",
            )?;

            for symbol in &diff.modified {
                let stable_id = compute_stable_symbol_id(symbol);

                stmt.execute(params![
                    symbol.start_line,
                    symbol.start_col,
                    symbol.end_line,
                    symbol.end_col,
                    symbol.start_byte,
                    symbol.end_byte,
                    symbol.docstring.as_deref(),
                    &stable_id,
                    graph_version,
                ])?;

                // Fetch the updated symbol to get its ID
                let id: i64 = tx.query_row(
                    "SELECT id FROM symbols WHERE stable_id = ? AND graph_version = ?",
                    params![&stable_id, graph_version],
                    |row| row.get(0),
                )?;

                all_symbols.push(Symbol {
                    id,
                    file_path: file_path.to_string(),
                    kind: symbol.kind.clone(),
                    name: symbol.name.clone(),
                    qualname: symbol.qualname.clone(),
                    start_line: symbol.start_line,
                    start_col: symbol.start_col,
                    end_line: symbol.end_line,
                    end_col: symbol.end_col,
                    start_byte: symbol.start_byte,
                    end_byte: symbol.end_byte,
                    signature: symbol.signature.clone(),
                    docstring: symbol.docstring.clone(),
                    graph_version,
                    commit_sha: commit_sha.map(str::to_string),
                    stable_id: Some(stable_id),
                });
            }
        }

        // PHASE 4: Fetch unchanged symbols (they're already in the database)
        // We need to return all symbols for edge resolution
        if !diff.unchanged.is_empty() {
            for symbol in &diff.unchanged {
                let stable_id = compute_stable_symbol_id(symbol);

                // Query the database for the unchanged symbol
                let existing = tx.query_row(
                    "SELECT id, kind, name, qualname, start_line, start_col, end_line, end_col,
                            start_byte, end_byte, signature, docstring, graph_version, commit_sha, stable_id
                     FROM symbols
                     WHERE stable_id = ? AND graph_version = ?",
                    params![&stable_id, graph_version],
                    |row| {
                        Ok(Symbol {
                            id: row.get(0)?,
                            file_path: file_path.to_string(),
                            kind: row.get(1)?,
                            name: row.get(2)?,
                            qualname: row.get(3)?,
                            start_line: row.get(4)?,
                            start_col: row.get(5)?,
                            end_line: row.get(6)?,
                            end_col: row.get(7)?,
                            start_byte: row.get(8)?,
                            end_byte: row.get(9)?,
                            signature: row.get(10)?,
                            docstring: row.get(11)?,
                            graph_version: row.get(12)?,
                            commit_sha: row.get(13)?,
                            stable_id: row.get(14)?,
                        })
                    }
                )?;

                all_symbols.push(existing);
            }
        }

        tx.commit()?;
        Ok(all_symbols)
    }

    /// Update symbols for multiple files in a single batch transaction
    ///
    /// This is the Phase 4 optimization: instead of one transaction per file,
    /// batch all file updates into a single transaction for maximum throughput.
    ///
    /// # Performance
    ///
    /// - Individual transactions: 100 files = 100 transactions (~200 files/sec)
    /// - Batch transaction: 100 files = 1 transaction (>500 files/sec target)
    ///
    /// # Arguments
    ///
    /// * `file_diffs` - Vector of file diffs to apply in batch
    ///
    /// # Returns
    ///
    /// HashMap mapping file_id to its symbols (for edge resolution)
    ///
    /// # Implementation
    ///
    /// This method collects all operations across all files and executes them
    /// in a single transaction:
    ///
    /// 1. Collect all deletes across all files → single batch DELETE
    /// 2. Collect all inserts across all files → batch INSERT with prepared statement
    /// 3. Collect all updates across all files → batch UPDATE with prepared statement
    /// 4. Fetch all unchanged symbols from database
    ///
    /// Transaction overhead is eliminated, resulting in 3-5x throughput improvement.
    pub fn update_files_symbols_batch(
        &mut self,
        file_diffs: &[crate::indexer::batch::FileDiff],
    ) -> Result<HashMap<i64, Vec<Symbol>>> {
        use crate::indexer::stable_id::compute_stable_symbol_id;

        if file_diffs.is_empty() {
            return Ok(HashMap::new());
        }

        let mut conn = self.conn();
        let tx = conn.transaction()?;

        // Result: map file_id -> symbols
        let mut file_symbols: HashMap<i64, Vec<Symbol>> = HashMap::new();

        // PHASE 1: Batch DELETE all removed symbols across all files
        let all_deleted: Vec<String> = file_diffs
            .iter()
            .flat_map(|fd| fd.diff.deleted.clone())
            .collect();

        if !all_deleted.is_empty() {
            let placeholders = vec!["?"; all_deleted.len()].join(",");
            let delete_sql = format!("DELETE FROM symbols WHERE stable_id IN ({})", placeholders);

            tx.execute(&delete_sql, rusqlite::params_from_iter(all_deleted.iter()))?;
        }

        // PHASE 2: Batch INSERT all new symbols across all files
        if file_diffs.iter().any(|fd| !fd.diff.added.is_empty()) {
            let mut stmt = tx.prepare_cached(
                "INSERT INTO symbols
                 (file_id, kind, name, qualname, start_line, start_col, end_line, end_col, start_byte, end_byte, signature, docstring, graph_version, commit_sha, stable_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )?;

            for fd in file_diffs {
                let mut symbols_for_file = Vec::new();

                for symbol in &fd.diff.added {
                    let stable_id = compute_stable_symbol_id(symbol);

                    stmt.execute(params![
                        fd.file_id,
                        &symbol.kind,
                        &symbol.name,
                        &symbol.qualname,
                        symbol.start_line,
                        symbol.start_col,
                        symbol.end_line,
                        symbol.end_col,
                        symbol.start_byte,
                        symbol.end_byte,
                        symbol.signature.as_deref(),
                        symbol.docstring.as_deref(),
                        fd.graph_version,
                        fd.commit_sha.as_deref(),
                        &stable_id,
                    ])?;

                    let id = tx.last_insert_rowid();
                    symbols_for_file.push(Symbol {
                        id,
                        file_path: fd.file_path.clone(),
                        kind: symbol.kind.clone(),
                        name: symbol.name.clone(),
                        qualname: symbol.qualname.clone(),
                        start_line: symbol.start_line,
                        start_col: symbol.start_col,
                        end_line: symbol.end_line,
                        end_col: symbol.end_col,
                        start_byte: symbol.start_byte,
                        end_byte: symbol.end_byte,
                        signature: symbol.signature.clone(),
                        docstring: symbol.docstring.clone(),
                        graph_version: fd.graph_version,
                        commit_sha: fd.commit_sha.clone(),
                        stable_id: Some(stable_id),
                    });
                }

                file_symbols
                    .entry(fd.file_id)
                    .or_default()
                    .extend(symbols_for_file);
            }
        }

        // PHASE 3: Batch UPDATE all modified symbols across all files
        if file_diffs.iter().any(|fd| !fd.diff.modified.is_empty()) {
            let mut stmt = tx.prepare_cached(
                "UPDATE symbols
                 SET start_line = ?, start_col = ?, end_line = ?, end_col = ?,
                     start_byte = ?, end_byte = ?, docstring = ?
                 WHERE stable_id = ? AND graph_version = ?",
            )?;

            for fd in file_diffs {
                let mut symbols_for_file = Vec::new();

                for symbol in &fd.diff.modified {
                    let stable_id = compute_stable_symbol_id(symbol);

                    stmt.execute(params![
                        symbol.start_line,
                        symbol.start_col,
                        symbol.end_line,
                        symbol.end_col,
                        symbol.start_byte,
                        symbol.end_byte,
                        symbol.docstring.as_deref(),
                        &stable_id,
                        fd.graph_version,
                    ])?;

                    // Fetch the updated symbol
                    let id: i64 = tx.query_row(
                        "SELECT id FROM symbols WHERE stable_id = ? AND graph_version = ?",
                        params![&stable_id, fd.graph_version],
                        |row| row.get(0),
                    )?;

                    symbols_for_file.push(Symbol {
                        id,
                        file_path: fd.file_path.clone(),
                        kind: symbol.kind.clone(),
                        name: symbol.name.clone(),
                        qualname: symbol.qualname.clone(),
                        start_line: symbol.start_line,
                        start_col: symbol.start_col,
                        end_line: symbol.end_line,
                        end_col: symbol.end_col,
                        start_byte: symbol.start_byte,
                        end_byte: symbol.end_byte,
                        signature: symbol.signature.clone(),
                        docstring: symbol.docstring.clone(),
                        graph_version: fd.graph_version,
                        commit_sha: fd.commit_sha.clone(),
                        stable_id: Some(stable_id),
                    });
                }

                file_symbols
                    .entry(fd.file_id)
                    .or_default()
                    .extend(symbols_for_file);
            }
        }

        // PHASE 4: Fetch unchanged symbols from database
        for fd in file_diffs {
            if !fd.diff.unchanged.is_empty() {
                let mut symbols_for_file = Vec::new();

                for symbol in &fd.diff.unchanged {
                    let stable_id = compute_stable_symbol_id(symbol);

                    let existing = tx.query_row(
                        "SELECT id, kind, name, qualname, start_line, start_col, end_line, end_col,
                                start_byte, end_byte, signature, docstring, graph_version, commit_sha, stable_id
                         FROM symbols
                         WHERE stable_id = ? AND graph_version = ?",
                        params![&stable_id, fd.graph_version],
                        |row| {
                            Ok(Symbol {
                                id: row.get(0)?,
                                file_path: fd.file_path.clone(),
                                kind: row.get(1)?,
                                name: row.get(2)?,
                                qualname: row.get(3)?,
                                start_line: row.get(4)?,
                                start_col: row.get(5)?,
                                end_line: row.get(6)?,
                                end_col: row.get(7)?,
                                start_byte: row.get(8)?,
                                end_byte: row.get(9)?,
                                signature: row.get(10)?,
                                docstring: row.get(11)?,
                                graph_version: row.get(12)?,
                                commit_sha: row.get(13)?,
                                stable_id: row.get(14)?,
                            })
                        }
                    )?;

                    symbols_for_file.push(existing);
                }

                file_symbols
                    .entry(fd.file_id)
                    .or_default()
                    .extend(symbols_for_file);
            }
        }

        tx.commit()?;
        Ok(file_symbols)
    }

    pub fn insert_edges(
        &mut self,
        file_id: i64,
        edges: &[EdgeInput],
        symbol_map: &HashMap<String, i64>,
        graph_version: i64,
        commit_sha: Option<&str>,
    ) -> Result<usize> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut count = 0;
        {
            let mut insert_stmt = tx.prepare(
                "INSERT INTO edges
                 (file_id, source_symbol_id, target_symbol_id, kind, target_qualname, detail, evidence_snippet,
                  evidence_start_line, evidence_end_line, confidence, graph_version, commit_sha, trace_id, span_id, event_ts)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            let mut exact_lookup_stmt =
                tx.prepare("SELECT id FROM symbols WHERE qualname = ? LIMIT 1")?;
            // Same-language fuzzy lookup: prefer symbols from files matching source language
            let mut fuzzy_same_lang_stmt = tx.prepare(
                "SELECT s.id
                 FROM symbols s
                 JOIN files f ON s.file_id = f.id
                 WHERE (s.qualname = ? OR s.qualname LIKE ?)
                   AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
                   AND s.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                   AND f.language = ?
                 ORDER BY CASE WHEN s.qualname = ? THEN 0 ELSE 1 END, LENGTH(s.qualname) ASC
                 LIMIT 1"
            )?;
            // Cross-language fuzzy lookup: fallback for bridge edges only
            let mut fuzzy_any_lang_stmt = tx.prepare(
                "SELECT s.id
                 FROM symbols s
                 JOIN files f ON s.file_id = f.id
                 WHERE (s.qualname = ? OR s.qualname LIKE ?)
                   AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
                   AND s.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                 ORDER BY CASE WHEN s.qualname = ? THEN 0 ELSE 1 END, LENGTH(s.qualname) ASC
                 LIMIT 1"
            )?;
            // Look up the source file's language for same-language preference
            let source_lang: String = tx
                .query_row(
                    "SELECT language FROM files WHERE id = ?",
                    params![file_id],
                    |row| row.get(0),
                )
                .unwrap_or_else(|_| "unknown".to_string());

            for edge in edges {
                let source_id =
                    resolve_symbol_id(&edge.source_qualname, symbol_map, &mut exact_lookup_stmt)?;
                let target_id =
                    resolve_symbol_id(&edge.target_qualname, symbol_map, &mut exact_lookup_stmt)?
                        .or_else(|| {
                            // Fuzzy fallback: try same-language first, then cross-language for bridge edges only
                            edge.target_qualname.as_ref().and_then(|qn| {
                                let method_name = qn.split('.').next_back().unwrap_or(qn);
                                let pattern = format!("%.{}", method_name);
                                // Try same-language first
                                let same_lang = fuzzy_same_lang_stmt
                                    .query_row(
                                        params![
                                            method_name,
                                            &pattern,
                                            graph_version,
                                            graph_version,
                                            &source_lang,
                                            method_name
                                        ],
                                        |row| row.get(0),
                                    )
                                    .optional()
                                    .ok()
                                    .flatten();
                                if same_lang.is_some() {
                                    return same_lang;
                                }
                                // Cross-language fallback only for bridge edge kinds
                                if is_bridge_edge_kind(&edge.kind) {
                                    fuzzy_any_lang_stmt
                                        .query_row(
                                            params![
                                                method_name,
                                                &pattern,
                                                graph_version,
                                                graph_version,
                                                method_name
                                            ],
                                            |row| row.get(0),
                                        )
                                        .optional()
                                        .ok()
                                        .flatten()
                                } else {
                                    None
                                }
                            })
                        });

                insert_stmt.execute(params![
                    file_id,
                    source_id,
                    target_id,
                    &edge.kind,
                    edge.target_qualname.as_deref(),
                    edge.detail.as_deref(),
                    edge.evidence_snippet.as_deref(),
                    edge.evidence_start_line,
                    edge.evidence_end_line,
                    edge.confidence,
                    graph_version,
                    commit_sha,
                    edge.trace_id.as_deref(),
                    edge.span_id.as_deref(),
                    edge.event_ts,
                ])?;
                count += 1;
            }
        }
        tx.commit()?;
        Ok(count)
    }

    /// Batch re-resolution of existing edges with NULL target_symbol_id
    ///
    /// This method attempts to resolve unresolved edges in two passes:
    /// 1. Exact match on target_qualname
    /// 2. Fuzzy suffix matching for remaining NULLs
    ///
    /// Processing is done in batches of 1000 rows to avoid long lock holds.
    pub fn resolve_null_target_edges(&self, graph_version: i64) -> Result<usize> {
        let mut total_resolved = 0;

        // First pass: exact match
        let exact_resolved = self.conn().execute(
            "UPDATE edges SET target_symbol_id = (
                SELECT s.id FROM symbols s
                WHERE s.qualname = edges.target_qualname
                AND s.graph_version = edges.graph_version
                LIMIT 1
            )
            WHERE target_symbol_id IS NULL
            AND target_qualname IS NOT NULL
            AND graph_version = ?",
            params![graph_version],
        )?;
        total_resolved += exact_resolved;

        // Second pass: fuzzy suffix matching in batches
        const BATCH_SIZE: usize = 1000;
        loop {
            let mut conn = self.conn();
            let tx = conn.transaction()?;

            // Find batch of unresolved edges (include source file language and edge kind)
            let unresolved: Vec<(i64, String, String, String)> = {
                let mut stmt = tx.prepare(
                    "SELECT e.id, e.target_qualname, COALESCE(f.language, 'unknown'), e.kind
                     FROM edges e
                     JOIN files f ON e.file_id = f.id
                     WHERE e.target_symbol_id IS NULL
                     AND e.target_qualname IS NOT NULL
                     AND e.graph_version = ?
                     LIMIT ?",
                )?;
                let rows = stmt.query_map(params![graph_version, BATCH_SIZE], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })?;
                rows.collect::<Result<Vec<_>, _>>()?
            };

            if unresolved.is_empty() {
                break;
            }

            let mut count = 0;
            {
                // Same-language fuzzy lookup
                let mut fuzzy_same_lang_stmt = tx.prepare(
                    "SELECT s.id
                     FROM symbols s
                     JOIN files f ON s.file_id = f.id
                     WHERE (s.qualname = ? OR s.qualname LIKE ?)
                       AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
                       AND s.graph_version = ?
                       AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                       AND f.language = ?
                     ORDER BY CASE WHEN s.qualname = ? THEN 0 ELSE 1 END, LENGTH(s.qualname) ASC
                     LIMIT 1"
                )?;
                // Cross-language fuzzy lookup (for bridge edges only)
                let mut fuzzy_any_lang_stmt = tx.prepare(
                    "SELECT s.id
                     FROM symbols s
                     JOIN files f ON s.file_id = f.id
                     WHERE (s.qualname = ? OR s.qualname LIKE ?)
                       AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
                       AND s.graph_version = ?
                       AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                     ORDER BY CASE WHEN s.qualname = ? THEN 0 ELSE 1 END, LENGTH(s.qualname) ASC
                     LIMIT 1"
                )?;

                let mut update_stmt =
                    tx.prepare("UPDATE edges SET target_symbol_id = ? WHERE id = ?")?;

                for (edge_id, target_qualname, source_lang, edge_kind) in &unresolved {
                    let method_name = target_qualname
                        .split('.')
                        .next_back()
                        .unwrap_or(target_qualname);
                    let pattern = format!("%.{}", method_name);

                    // Try same-language first
                    let resolved = fuzzy_same_lang_stmt
                        .query_row(
                            params![
                                method_name,
                                &pattern,
                                graph_version,
                                graph_version,
                                source_lang,
                                method_name
                            ],
                            |row| row.get::<_, i64>(0),
                        )
                        .optional()?
                        .or_else(|| {
                            // Cross-language fallback only for bridge edges
                            if is_bridge_edge_kind(edge_kind) {
                                fuzzy_any_lang_stmt
                                    .query_row(
                                        params![
                                            method_name,
                                            &pattern,
                                            graph_version,
                                            graph_version,
                                            method_name
                                        ],
                                        |row| row.get::<_, i64>(0),
                                    )
                                    .optional()
                                    .ok()
                                    .flatten()
                            } else {
                                None
                            }
                        });

                    if let Some(symbol_id) = resolved {
                        update_stmt.execute(params![symbol_id, edge_id])?;
                        count += 1;
                    }
                }
            } // stmts dropped here

            tx.commit()?;
            total_resolved += count;

            if count == 0 {
                break;
            }
        }

        Ok(total_resolved)
    }

    pub fn upsert_file_metrics(&mut self, file_id: i64, metrics: &FileMetricsInput) -> Result<()> {
        self.conn().execute(
            "INSERT INTO file_metrics (file_id, loc, blank, comment, code)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(file_id) DO UPDATE SET
                loc = excluded.loc,
                blank = excluded.blank,
                comment = excluded.comment,
                code = excluded.code",
            params![
                file_id,
                metrics.loc,
                metrics.blank,
                metrics.comment,
                metrics.code
            ],
        )?;
        Ok(())
    }

    pub fn insert_symbol_metrics(
        &mut self,
        file_id: i64,
        metrics: &[SymbolMetricsInput],
        symbol_map: &HashMap<String, i64>,
    ) -> Result<usize> {
        if metrics.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut count = 0;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO symbol_metrics
                 (symbol_id, file_id, loc, complexity, duplication_hash)
                 VALUES (?, ?, ?, ?, ?)
                 ON CONFLICT(symbol_id) DO UPDATE SET
                    file_id = excluded.file_id,
                    loc = excluded.loc,
                    complexity = excluded.complexity,
                    duplication_hash = excluded.duplication_hash",
            )?;
            for metric in metrics {
                let Some(symbol_id) = symbol_map.get(&metric.qualname) else {
                    continue;
                };
                stmt.execute(params![
                    symbol_id,
                    file_id,
                    metric.loc,
                    metric.complexity,
                    metric.duplication_hash.as_deref(),
                ])?;
                count += 1;
            }
        }
        tx.commit()?;
        Ok(count)
    }

    pub fn get_symbol_by_id(&self, id: i64) -> Result<Option<Symbol>> {
        self.read_conn()?
            .query_row(
                "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                        s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                        s.graph_version, s.commit_sha, s.stable_id
                 FROM symbols s
                 JOIN files f ON s.file_id = f.id
                 WHERE s.id = ?",
                params![id],
                symbol_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn get_symbol_by_qualname(
        &self,
        qualname: &str,
        graph_version: i64,
    ) -> Result<Option<Symbol>> {
        self.read_conn()?
            .query_row(
                "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                        s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                        s.graph_version, s.commit_sha, s.stable_id
                 FROM symbols s
                 JOIN files f ON s.file_id = f.id
                 WHERE s.qualname = ?
                   AND s.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                 LIMIT 1",
                params![qualname, graph_version, graph_version],
                symbol_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Get all symbols for a file by path
    ///
    /// # Arguments
    ///
    /// * `file_path` - The relative file path
    /// * `graph_version` - The graph version to query
    ///
    /// # Returns
    ///
    /// A vector of all symbols in the file for the specified graph version
    pub fn get_symbols_for_file(&self, file_path: &str, graph_version: i64) -> Result<Vec<Symbol>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE f.path = ?
               AND s.graph_version = ?
             ORDER BY s.start_line",
        )?;

        let rows = stmt.query_map(params![file_path, graph_version], |row| {
            symbol_from_row(row)
        })?;

        let mut symbols = Vec::new();
        for row in rows {
            symbols.push(row?);
        }

        Ok(symbols)
    }

    /// Get a symbol by stable_id from a specific graph version
    ///
    /// This is useful for comparing symbols across versions to detect signature changes
    pub fn get_symbol_by_stable_id(
        &self,
        stable_id: &str,
        graph_version: i64,
    ) -> Result<Option<Symbol>> {
        self.read_conn()?
            .query_row(
                "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                        s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                        s.graph_version, s.commit_sha, s.stable_id
                 FROM symbols s
                 JOIN files f ON s.file_id = f.id
                 WHERE s.stable_id = ?
                   AND s.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                 LIMIT 1",
                params![stable_id, graph_version, graph_version],
                symbol_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn enclosing_symbol_for_line(
        &self,
        path: &str,
        line: i64,
        graph_version: i64,
    ) -> Result<Option<Symbol>> {
        self.read_conn()?
            .query_row(
                "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                        s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                        s.graph_version, s.commit_sha, s.stable_id
                 FROM symbols s
                 JOIN files f ON s.file_id = f.id
                 WHERE f.path = ? AND s.start_line <= ? AND s.end_line >= ?
                   AND s.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                 ORDER BY CASE WHEN s.kind = 'module' THEN 1 ELSE 0 END,
                          (s.end_line - s.start_line) ASC,
                          s.start_line DESC
                 LIMIT 1",
                params![path, line, line, graph_version, graph_version],
                symbol_from_row,
            )
            .optional()
            .map_err(Into::into)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn list_edges(
        &self,
        limit: usize,
        offset: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        kinds: Option<&[String]>,
        source_id: Option<i64>,
        target_id: Option<i64>,
        target_qualname: Option<&String>,
        resolved_only: bool,
        min_confidence: Option<f64>,
        graph_version: i64,
        trace_id: Option<&String>,
        event_after: Option<i64>,
        event_before: Option<i64>,
    ) -> Result<Vec<Edge>> {
        let mut sql = String::from(
            "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                    e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];
        let source_id_param = source_id;
        if let Some(source_id) = source_id_param.as_ref() {
            sql.push_str(" AND e.source_symbol_id = ?");
            params.push(source_id as &dyn rusqlite::ToSql);
        }
        let target_id_param = target_id;
        if let Some(target_id) = target_id_param.as_ref() {
            sql.push_str(" AND e.target_symbol_id = ?");
            params.push(target_id as &dyn rusqlite::ToSql);
        } else if let Some(target_qualname) = target_qualname {
            sql.push_str(" AND e.target_qualname = ?");
            params.push(target_qualname);
        }
        if resolved_only {
            sql.push_str(" AND e.source_symbol_id IS NOT NULL AND e.target_symbol_id IS NOT NULL");
        }
        if let Some(kinds) = kinds {
            if kinds.is_empty() {
                return Ok(Vec::new());
            }
            sql.push_str(" AND e.kind IN (");
            for (idx, _) in kinds.iter().enumerate() {
                if idx > 0 {
                    sql.push(',');
                }
                sql.push('?');
            }
            sql.push(')');
            for kind in kinds {
                params.push(kind as &dyn rusqlite::ToSql);
            }
        }
        if let Some(languages) = languages
            && !languages.is_empty()
        {
            sql.push_str(" AND f.language IN (");
            for (idx, _) in languages.iter().enumerate() {
                if idx > 0 {
                    sql.push(',');
                }
                sql.push('?');
            }
            sql.push(')');
            for language in languages {
                params.push(language as &dyn rusqlite::ToSql);
            }
        }
        let min_confidence_param = min_confidence;
        if let Some(min_confidence) = min_confidence_param.as_ref() {
            sql.push_str(" AND e.confidence >= ?");
            params.push(min_confidence as &dyn rusqlite::ToSql);
        }
        if let Some(trace_id) = trace_id {
            sql.push_str(" AND e.trace_id = ?");
            params.push(trace_id);
        }
        let event_after_param = event_after;
        if let Some(event_after) = event_after_param.as_ref() {
            sql.push_str(" AND e.event_ts >= ?");
            params.push(event_after as &dyn rusqlite::ToSql);
        }
        let event_before_param = event_before;
        if let Some(event_before) = event_before_param.as_ref() {
            sql.push_str(" AND e.event_ts <= ?");
            params.push(event_before as &dyn rusqlite::ToSql);
        }
        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");
        sql.push_str(" ORDER BY f.path, COALESCE(e.evidence_start_line, 0), e.id");
        sql.push_str(" LIMIT ? OFFSET ?");
        let limit = limit as i64;
        let offset = offset as i64;
        params.push(&limit);
        params.push(&offset);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, edge_from_row)?;
        let mut edges = Vec::new();
        for row in rows {
            edges.push(row?);
        }
        Ok(edges)
    }

    pub fn current_graph_version(&self) -> Result<i64> {
        let value = self.get_meta_i64("graph_version")?;
        Ok(value.unwrap_or(1))
    }

    pub fn graph_version_commit(&self, graph_version: i64) -> Result<Option<String>> {
        let value: Option<Option<String>> = self
            .read_conn()?
            .query_row(
                "SELECT commit_sha FROM graph_versions WHERE id = ?",
                params![graph_version],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?;
        Ok(value.flatten())
    }

    pub fn create_graph_version(&self, commit_sha: Option<&str>) -> Result<i64> {
        let created = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.conn().execute(
            "INSERT INTO graph_versions (created, commit_sha) VALUES (?, ?)",
            params![created, commit_sha],
        )?;
        let id = self.conn().last_insert_rowid();
        self.set_meta_i64("graph_version", id)?;
        Ok(id)
    }

    pub fn list_graph_versions(&self, limit: usize, offset: usize) -> Result<Vec<GraphVersion>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, created, commit_sha
             FROM graph_versions
             ORDER BY id DESC
             LIMIT ? OFFSET ?",
        )?;
        let limit = limit as i64;
        let offset = offset as i64;
        let rows = stmt.query_map(params![limit, offset], |row| {
            Ok(GraphVersion {
                id: row.get(0)?,
                created: row.get(1)?,
                commit_sha: row.get(2)?,
            })
        })?;
        let mut versions = Vec::new();
        for row in rows {
            versions.push(row?);
        }
        Ok(versions)
    }

    pub fn get_meta_i64(&self, key: &str) -> Result<Option<i64>> {
        let value: Option<String> = self
            .read_conn()?
            .query_row(
                "SELECT value FROM meta WHERE key = ?",
                params![key],
                |row| row.get(0),
            )
            .optional()?;
        Ok(value.and_then(|v| v.parse::<i64>().ok()))
    }

    pub fn set_meta_i64(&self, key: &str, value: i64) -> Result<()> {
        self.conn().execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value.to_string()],
        )?;
        Ok(())
    }
}

fn collect_path_prefixes(paths: Option<&[String]>) -> Vec<String> {
    let mut prefixes = Vec::new();
    let Some(paths) = paths else {
        return prefixes;
    };
    for raw in paths {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let prefix = trimmed.trim_end_matches('/');
        if prefix.is_empty() || prefix == "." {
            continue;
        }
        prefixes.push(prefix.to_string());
    }
    prefixes
}

fn append_path_filters<'a>(
    sql: &mut String,
    params: &mut Vec<&'a dyn rusqlite::ToSql>,
    path_params: &'a mut Vec<String>,
    paths: Option<&'a [String]>,
    table_alias: &str,
) {
    let prefixes = collect_path_prefixes(paths);
    if prefixes.is_empty() {
        return;
    }
    path_params.reserve(prefixes.len().saturating_mul(2));
    sql.push_str(" AND (");
    let base = path_params.len();
    for (idx, prefix) in prefixes.iter().enumerate() {
        if idx > 0 {
            sql.push_str(" OR ");
        }
        sql.push_str(table_alias);
        sql.push_str(".path = ? OR ");
        sql.push_str(table_alias);
        sql.push_str(".path LIKE ? ESCAPE '\\'");
        path_params.push(prefix.clone());
        let escaped = escape_like(prefix);
        path_params.push(format!("{escaped}/%"));
    }
    sql.push(')');
    for param in &path_params[base..] {
        params.push(param as &dyn rusqlite::ToSql);
    }
}

fn escape_like(raw: &str) -> String {
    let mut out = String::new();
    for ch in raw.chars() {
        match ch {
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

fn extract_target_name(test_name: &str) -> String {
    // Extract target name from test function name
    // Patterns:
    // - test_foo_bar -> foo_bar
    // - TestFooBar -> FooBar
    // - test_Foo_Bar -> Foo_Bar
    // - foo_test -> foo
    // - fooTest -> foo

    let name = test_name;

    // Remove "test_" or "Test" prefix
    let without_prefix = if let Some(stripped) = name.strip_prefix("test_") {
        stripped
    } else if let Some(stripped) = name.strip_prefix("Test") {
        stripped
    } else if let Some(stripped) = name.strip_suffix("_test") {
        stripped
    } else if let Some(stripped) = name.strip_suffix("Test") {
        stripped
    } else if let Some(stripped) = name.strip_suffix("Tests") {
        stripped
    } else {
        name
    };

    without_prefix.to_string()
}

fn symbol_from_row(row: &Row<'_>) -> rusqlite::Result<Symbol> {
    Ok(Symbol {
        id: row.get(0)?,
        file_path: row.get(1)?,
        kind: row.get(2)?,
        name: row.get(3)?,
        qualname: row.get(4)?,
        start_line: row.get(5)?,
        start_col: row.get(6)?,
        end_line: row.get(7)?,
        end_col: row.get(8)?,
        start_byte: row.get(9)?,
        end_byte: row.get(10)?,
        signature: row.get(11)?,
        docstring: row.get(12)?,
        graph_version: row.get(13)?,
        commit_sha: row.get(14)?,
        stable_id: row.get(15)?,
    })
}

fn symbol_from_row_offset(row: &Row<'_>, offset: usize) -> rusqlite::Result<Symbol> {
    Ok(Symbol {
        id: row.get(offset)?,
        file_path: row.get(offset + 1)?,
        kind: row.get(offset + 2)?,
        name: row.get(offset + 3)?,
        qualname: row.get(offset + 4)?,
        start_line: row.get(offset + 5)?,
        start_col: row.get(offset + 6)?,
        end_line: row.get(offset + 7)?,
        end_col: row.get(offset + 8)?,
        start_byte: row.get(offset + 9)?,
        end_byte: row.get(offset + 10)?,
        signature: row.get(offset + 11)?,
        docstring: row.get(offset + 12)?,
        graph_version: row.get(offset + 13)?,
        commit_sha: row.get(offset + 14)?,
        stable_id: row.get(offset + 15)?,
    })
}

fn edge_from_row(row: &Row<'_>) -> rusqlite::Result<Edge> {
    Ok(Edge {
        id: row.get(0)?,
        file_path: row.get(1)?,
        kind: row.get(2)?,
        source_symbol_id: row.get(3)?,
        target_symbol_id: row.get(4)?,
        target_qualname: row.get(5)?,
        detail: row.get(6)?,
        evidence_snippet: row.get(7)?,
        evidence_start_line: row.get(8)?,
        evidence_end_line: row.get(9)?,
        confidence: row.get(10)?,
        graph_version: row.get(11)?,
        commit_sha: row.get(12)?,
        trace_id: row.get(13)?,
        span_id: row.get(14)?,
        event_ts: row.get(15)?,
    })
}

fn resolve_symbol_id(
    qualname: &Option<String>,
    symbol_map: &HashMap<String, i64>,
    stmt: &mut rusqlite::Statement<'_>,
) -> Result<Option<i64>> {
    let name = match qualname.as_ref() {
        Some(name) => name,
        None => return Ok(None),
    };
    if let Some(id) = symbol_map.get(name) {
        return Ok(Some(*id));
    }
    let id = stmt.query_row(params![name], |row| row.get(0)).optional()?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::extract::SymbolInput;
    use tempfile::TempDir;

    fn create_test_db() -> (Db, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Db::new(&db_path).unwrap();
        (db, temp_dir)
    }

    fn make_test_symbol(
        qualname: &str,
        signature: Option<&str>,
        kind: &str,
        start_line: i64,
    ) -> SymbolInput {
        SymbolInput {
            kind: kind.to_string(),
            name: qualname.split('.').last().unwrap_or(qualname).to_string(),
            qualname: qualname.to_string(),
            start_line,
            start_col: 0,
            end_line: start_line + 5,
            end_col: 0,
            start_byte: 0,
            end_byte: 100,
            signature: signature.map(String::from),
            docstring: None,
        }
    }

    #[test]
    fn test_stable_id_stored_and_retrieved() {
        let (mut db, _temp) = create_test_db();

        // Insert a file
        let file_id = db
            .upsert_file("test.py", "abc123", "python", 100, 0)
            .unwrap();

        // Create test symbols
        let symbols = vec![
            make_test_symbol("test.function1", Some("(x: int) -> int"), "function", 10),
            make_test_symbol("test.function2", Some("(y: str) -> bool"), "function", 20),
        ];

        // Insert symbols
        let inserted = db
            .insert_symbols(file_id, "test.py", &symbols, 1, None)
            .unwrap();

        // Verify stable IDs were generated and stored
        assert_eq!(inserted.len(), 2);
        assert!(inserted[0].stable_id.is_some());
        assert!(inserted[1].stable_id.is_some());
        assert_ne!(inserted[0].stable_id, inserted[1].stable_id);

        // Retrieve symbols and verify stable IDs persist
        let retrieved = db.get_symbols_for_file("test.py", 1).unwrap();
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved[0].stable_id, inserted[0].stable_id);
        assert_eq!(retrieved[1].stable_id, inserted[1].stable_id);
    }

    #[test]
    fn test_stable_id_survives_line_number_changes() {
        use crate::indexer::stable_id::compute_stable_symbol_id;

        // Same symbol at different line numbers should have same stable ID
        let sym1 = make_test_symbol(
            "test.MyClass.method",
            Some("(x: int) -> int"),
            "function",
            10,
        );
        let sym2 = make_test_symbol(
            "test.MyClass.method",
            Some("(x: int) -> int"),
            "function",
            100,
        );

        let id1 = compute_stable_symbol_id(&sym1);
        let id2 = compute_stable_symbol_id(&sym2);

        assert_eq!(
            id1, id2,
            "Stable IDs should match despite different line numbers"
        );
    }

    #[test]
    fn test_stable_id_changes_with_signature() {
        use crate::indexer::stable_id::compute_stable_symbol_id;

        // Same qualname but different signature should have different stable IDs
        let sym1 = make_test_symbol(
            "test.MyClass.method",
            Some("(x: int) -> int"),
            "function",
            10,
        );
        let sym2 = make_test_symbol(
            "test.MyClass.method",
            Some("(x: str) -> int"),
            "function",
            10,
        );

        let id1 = compute_stable_symbol_id(&sym1);
        let id2 = compute_stable_symbol_id(&sym2);

        assert_ne!(id1, id2, "Stable IDs should differ when signature changes");
    }

    #[test]
    fn test_no_stable_id_hash_collisions() {
        use crate::indexer::stable_id::compute_stable_symbol_id;
        use std::collections::HashSet;

        // Generate many symbols and ensure no collisions
        let mut seen_ids = HashSet::new();
        let mut symbols = Vec::new();

        // Create 1000 different symbols
        for i in 0..1000 {
            let qualname = format!("test.Class{}.method{}", i / 10, i % 10);
            let signature = format!("(arg{}: int) -> int", i);
            symbols.push(make_test_symbol(
                &qualname,
                Some(&signature),
                "function",
                10,
            ));
        }

        for symbol in &symbols {
            let stable_id = compute_stable_symbol_id(symbol);
            assert!(
                seen_ids.insert(stable_id.clone()),
                "Hash collision detected for stable_id: {}",
                stable_id
            );
        }

        assert_eq!(seen_ids.len(), 1000, "Should have 1000 unique stable IDs");
    }

    #[test]
    fn test_backward_compatibility_integer_ids() {
        let (mut db, _temp) = create_test_db();

        // Insert a file
        let file_id = db
            .upsert_file("test.py", "abc123", "python", 100, 0)
            .unwrap();

        // Create and insert symbols
        let symbols = vec![
            make_test_symbol("test.function1", Some("(x: int) -> int"), "function", 10),
            make_test_symbol("test.function2", Some("(y: str) -> bool"), "function", 20),
        ];

        let inserted = db
            .insert_symbols(file_id, "test.py", &symbols, 1, None)
            .unwrap();

        // Verify integer IDs are still assigned and unique
        assert!(inserted[0].id > 0);
        assert!(inserted[1].id > 0);
        assert_ne!(inserted[0].id, inserted[1].id);

        // Verify we can look up by integer ID
        let by_id = db.get_symbol_by_id(inserted[0].id).unwrap().unwrap();
        assert_eq!(by_id.id, inserted[0].id);
        assert_eq!(by_id.qualname, "test.function1");

        // Verify we can look up by qualname (uses integer ID internally)
        let by_qualname = db
            .get_symbol_by_qualname("test.function1", 1)
            .unwrap()
            .unwrap();
        assert_eq!(by_qualname.id, inserted[0].id);
    }

    #[test]
    fn test_stable_id_format_validation() {
        use crate::indexer::stable_id::compute_stable_symbol_id;

        let symbol = make_test_symbol("test.function", Some("() -> None"), "function", 10);
        let stable_id = compute_stable_symbol_id(&symbol);

        // Verify format: sym_{16_hex_chars}
        assert!(
            stable_id.starts_with("sym_"),
            "Stable ID should start with 'sym_'"
        );
        assert_eq!(
            stable_id.len(),
            20,
            "Stable ID should be 20 chars: 'sym_' + 16 hex"
        );

        let hex_part = &stable_id[4..];
        assert!(
            hex_part.chars().all(|c| c.is_ascii_hexdigit()),
            "Stable ID suffix should be valid hexadecimal"
        );
    }

    #[test]
    fn test_database_migration_adds_stable_id_column() {
        let (db, _temp) = create_test_db();

        // Verify the stable_id column exists by checking the schema
        let conn = db.read_conn().unwrap();
        let mut stmt = conn.prepare("PRAGMA table_info(symbols)").unwrap();
        let columns: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(
            columns.contains(&"stable_id".to_string()),
            "symbols table should have stable_id column after migration"
        );
    }

    #[test]
    fn test_stable_id_index_exists() {
        let (db, _temp) = create_test_db();

        // Verify the index on stable_id exists
        let conn = db.read_conn().unwrap();
        let mut stmt = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='symbols'")
            .unwrap();
        let indexes: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(
            indexes.contains(&"idx_symbols_stable_id".to_string()),
            "Should have index idx_symbols_stable_id"
        );
    }

    // ========== PHASE 3 TESTS: Incremental Database Updates ==========

    #[test]
    fn test_update_file_symbols_only_updates_changed() {
        use crate::indexer::differ::compute_symbol_diff;

        let (mut db, _temp) = create_test_db();

        // Insert a file with initial symbols
        let file_id = db
            .upsert_file("test.py", "abc123", "python", 100, 0)
            .unwrap();

        let initial_symbols = vec![
            make_test_symbol("test.func1", Some("(x: int) -> int"), "function", 10),
            make_test_symbol("test.func2", Some("(y: str) -> bool"), "function", 20),
            make_test_symbol("test.func3", Some("(z: float) -> None"), "function", 30),
        ];

        // Insert initial symbols
        db.insert_symbols(file_id, "test.py", &initial_symbols, 1, None)
            .unwrap();

        // Simulate a change: func2 moved to different line, func3 has new docstring
        let updated_symbols = vec![
            make_test_symbol("test.func1", Some("(x: int) -> int"), "function", 10), // unchanged
            make_test_symbol("test.func2", Some("(y: str) -> bool"), "function", 25), // line changed
            // func3 with docstring
            SymbolInput {
                kind: "function".to_string(),
                name: "func3".to_string(),
                qualname: "test.func3".to_string(),
                start_line: 30,
                start_col: 0,
                end_line: 35,
                end_col: 0,
                start_byte: 0,
                end_byte: 100,
                signature: Some("(z: float) -> None".to_string()),
                docstring: Some("This is a docstring".to_string()),
            },
        ];

        // Compute diff
        let existing = db.get_symbols_for_file("test.py", 1).unwrap();
        let diff = compute_symbol_diff(existing, updated_symbols.clone());

        // Verify diff is correct
        assert_eq!(diff.added.len(), 0, "No symbols added");
        assert_eq!(diff.modified.len(), 2, "func2 and func3 modified");
        assert_eq!(diff.deleted.len(), 0, "No symbols deleted");
        assert_eq!(diff.unchanged.len(), 1, "func1 unchanged");

        // Apply update
        let result = db
            .update_file_symbols(file_id, "test.py", diff, 1, None)
            .unwrap();

        // Verify result contains all symbols
        assert_eq!(result.len(), 3);

        // Verify database state
        let final_symbols = db.get_symbols_for_file("test.py", 1).unwrap();
        assert_eq!(final_symbols.len(), 3);

        // Check func1 unchanged (same line)
        let func1 = final_symbols
            .iter()
            .find(|s| s.qualname == "test.func1")
            .unwrap();
        assert_eq!(func1.start_line, 10);

        // Check func2 updated (new line)
        let func2 = final_symbols
            .iter()
            .find(|s| s.qualname == "test.func2")
            .unwrap();
        assert_eq!(func2.start_line, 25);

        // Check func3 updated (new docstring)
        let func3 = final_symbols
            .iter()
            .find(|s| s.qualname == "test.func3")
            .unwrap();
        assert_eq!(func3.docstring, Some("This is a docstring".to_string()));
    }

    #[test]
    fn test_update_file_symbols_adds_new_symbol() {
        use crate::indexer::differ::compute_symbol_diff;

        let (mut db, _temp) = create_test_db();

        let file_id = db
            .upsert_file("test.py", "abc123", "python", 100, 0)
            .unwrap();

        // Start with 2 symbols
        let initial_symbols = vec![
            make_test_symbol("test.func1", Some("(x: int) -> int"), "function", 10),
            make_test_symbol("test.func2", Some("(y: str) -> bool"), "function", 20),
        ];

        db.insert_symbols(file_id, "test.py", &initial_symbols, 1, None)
            .unwrap();

        // Add a third symbol
        let updated_symbols = vec![
            make_test_symbol("test.func1", Some("(x: int) -> int"), "function", 10),
            make_test_symbol("test.func2", Some("(y: str) -> bool"), "function", 20),
            make_test_symbol("test.func3", Some("(z: float) -> None"), "function", 30), // NEW
        ];

        let existing = db.get_symbols_for_file("test.py", 1).unwrap();
        let diff = compute_symbol_diff(existing, updated_symbols.clone());

        assert_eq!(diff.added.len(), 1, "One symbol added");
        assert_eq!(diff.modified.len(), 0, "No symbols modified");
        assert_eq!(diff.deleted.len(), 0, "No symbols deleted");
        assert_eq!(diff.unchanged.len(), 2, "Two symbols unchanged");

        let result = db
            .update_file_symbols(file_id, "test.py", diff, 1, None)
            .unwrap();

        assert_eq!(result.len(), 3, "Should have 3 symbols now");

        let final_symbols = db.get_symbols_for_file("test.py", 1).unwrap();
        assert_eq!(final_symbols.len(), 3);
    }

    #[test]
    fn test_update_file_symbols_deletes_removed_symbol() {
        use crate::indexer::differ::compute_symbol_diff;

        let (mut db, _temp) = create_test_db();

        let file_id = db
            .upsert_file("test.py", "abc123", "python", 100, 0)
            .unwrap();

        // Start with 3 symbols
        let initial_symbols = vec![
            make_test_symbol("test.func1", Some("(x: int) -> int"), "function", 10),
            make_test_symbol("test.func2", Some("(y: str) -> bool"), "function", 20),
            make_test_symbol("test.func3", Some("(z: float) -> None"), "function", 30),
        ];

        db.insert_symbols(file_id, "test.py", &initial_symbols, 1, None)
            .unwrap();

        // Remove func2
        let updated_symbols = vec![
            make_test_symbol("test.func1", Some("(x: int) -> int"), "function", 10),
            make_test_symbol("test.func3", Some("(z: float) -> None"), "function", 30),
        ];

        let existing = db.get_symbols_for_file("test.py", 1).unwrap();
        let diff = compute_symbol_diff(existing, updated_symbols.clone());

        assert_eq!(diff.added.len(), 0, "No symbols added");
        assert_eq!(diff.modified.len(), 0, "No symbols modified");
        assert_eq!(diff.deleted.len(), 1, "One symbol deleted");
        assert_eq!(diff.unchanged.len(), 2, "Two symbols unchanged");

        let result = db
            .update_file_symbols(file_id, "test.py", diff, 1, None)
            .unwrap();

        assert_eq!(result.len(), 2, "Should have 2 symbols now");

        let final_symbols = db.get_symbols_for_file("test.py", 1).unwrap();
        assert_eq!(final_symbols.len(), 2);
        assert!(!final_symbols.iter().any(|s| s.qualname == "test.func2"));
    }

    #[test]
    fn test_update_file_symbols_no_changes_no_operations() {
        use crate::indexer::differ::compute_symbol_diff;

        let (mut db, _temp) = create_test_db();

        let file_id = db
            .upsert_file("test.py", "abc123", "python", 100, 0)
            .unwrap();

        let symbols = vec![
            make_test_symbol("test.func1", Some("(x: int) -> int"), "function", 10),
            make_test_symbol("test.func2", Some("(y: str) -> bool"), "function", 20),
        ];

        db.insert_symbols(file_id, "test.py", &symbols, 1, None)
            .unwrap();

        // Same symbols, no changes
        let unchanged_symbols = symbols.clone();

        let existing = db.get_symbols_for_file("test.py", 1).unwrap();
        let diff = compute_symbol_diff(existing, unchanged_symbols);

        assert_eq!(diff.added.len(), 0, "No symbols added");
        assert_eq!(diff.modified.len(), 0, "No symbols modified");
        assert_eq!(diff.deleted.len(), 0, "No symbols deleted");
        assert_eq!(diff.unchanged.len(), 2, "All symbols unchanged");

        let result = db
            .update_file_symbols(file_id, "test.py", diff, 1, None)
            .unwrap();

        assert_eq!(result.len(), 2, "Should still have 2 symbols");

        let final_symbols = db.get_symbols_for_file("test.py", 1).unwrap();
        assert_eq!(final_symbols.len(), 2);
    }

    #[test]
    fn test_update_file_symbols_mixed_changes() {
        use crate::indexer::differ::compute_symbol_diff;

        let (mut db, _temp) = create_test_db();

        let file_id = db
            .upsert_file("test.py", "abc123", "python", 100, 0)
            .unwrap();

        // Initial state: func1, func2, func3
        let initial_symbols = vec![
            make_test_symbol("test.func1", Some("(x: int) -> int"), "function", 10),
            make_test_symbol("test.func2", Some("(y: str) -> bool"), "function", 20),
            make_test_symbol("test.func3", Some("(z: float) -> None"), "function", 30),
        ];

        db.insert_symbols(file_id, "test.py", &initial_symbols, 1, None)
            .unwrap();

        // Updated state:
        // - func1 unchanged
        // - func2 deleted
        // - func3 modified (line changed)
        // - func4 added
        let updated_symbols = vec![
            make_test_symbol("test.func1", Some("(x: int) -> int"), "function", 10), // unchanged
            make_test_symbol("test.func3", Some("(z: float) -> None"), "function", 35), // modified
            make_test_symbol("test.func4", Some("(a: bool) -> int"), "function", 40), // added
        ];

        let existing = db.get_symbols_for_file("test.py", 1).unwrap();
        let diff = compute_symbol_diff(existing, updated_symbols.clone());

        assert_eq!(diff.added.len(), 1, "func4 added");
        assert_eq!(diff.modified.len(), 1, "func3 modified");
        assert_eq!(diff.deleted.len(), 1, "func2 deleted");
        assert_eq!(diff.unchanged.len(), 1, "func1 unchanged");

        let result = db
            .update_file_symbols(file_id, "test.py", diff, 1, None)
            .unwrap();

        assert_eq!(result.len(), 3, "Should have 3 symbols now");

        let final_symbols = db.get_symbols_for_file("test.py", 1).unwrap();
        assert_eq!(final_symbols.len(), 3);

        // Verify final state
        assert!(final_symbols.iter().any(|s| s.qualname == "test.func1"));
        assert!(!final_symbols.iter().any(|s| s.qualname == "test.func2")); // deleted
        assert!(final_symbols.iter().any(|s| s.qualname == "test.func3"));
        assert!(final_symbols.iter().any(|s| s.qualname == "test.func4")); // added

        // Check func3 line was updated
        let func3 = final_symbols
            .iter()
            .find(|s| s.qualname == "test.func3")
            .unwrap();
        assert_eq!(func3.start_line, 35);
    }

    #[test]
    fn test_update_file_symbols_preserves_integer_ids() {
        use crate::indexer::differ::compute_symbol_diff;

        let (mut db, _temp) = create_test_db();

        let file_id = db
            .upsert_file("test.py", "abc123", "python", 100, 0)
            .unwrap();

        let initial_symbols = vec![make_test_symbol(
            "test.func1",
            Some("(x: int) -> int"),
            "function",
            10,
        )];

        let inserted = db
            .insert_symbols(file_id, "test.py", &initial_symbols, 1, None)
            .unwrap();

        let original_id = inserted[0].id;

        // Update func1 (line changed)
        let updated_symbols = vec![make_test_symbol(
            "test.func1",
            Some("(x: int) -> int"),
            "function",
            15,
        )];

        let existing = db.get_symbols_for_file("test.py", 1).unwrap();
        let diff = compute_symbol_diff(existing, updated_symbols);

        let result = db
            .update_file_symbols(file_id, "test.py", diff, 1, None)
            .unwrap();

        // Verify the integer ID was preserved
        assert_eq!(
            result[0].id, original_id,
            "Integer ID should be preserved on update"
        );

        let final_symbols = db.get_symbols_for_file("test.py", 1).unwrap();
        assert_eq!(final_symbols[0].id, original_id);
        assert_eq!(final_symbols[0].start_line, 15); // but line updated
    }

    #[test]
    fn test_update_files_symbols_batch() {
        use crate::indexer::batch::FileDiff;
        use crate::indexer::differ::compute_symbol_diff;

        let (mut db, _temp) = create_test_db();

        // Create 3 files
        let file1_id = db
            .upsert_file("test1.py", "abc1", "python", 100, 0)
            .unwrap();
        let file2_id = db
            .upsert_file("test2.py", "abc2", "python", 100, 0)
            .unwrap();
        let file3_id = db
            .upsert_file("test3.py", "abc3", "python", 100, 0)
            .unwrap();

        // Insert initial symbols for each file
        let file1_symbols = vec![
            make_test_symbol("test1.func1", Some("() -> None"), "function", 10),
            make_test_symbol("test1.func2", Some("() -> None"), "function", 20),
        ];
        let file2_symbols = vec![make_test_symbol(
            "test2.func1",
            Some("() -> None"),
            "function",
            10,
        )];
        let file3_symbols = vec![
            make_test_symbol("test3.func1", Some("() -> None"), "function", 10),
            make_test_symbol("test3.func2", Some("() -> None"), "function", 20),
            make_test_symbol("test3.func3", Some("() -> None"), "function", 30),
        ];

        db.insert_symbols(file1_id, "test1.py", &file1_symbols, 1, None)
            .unwrap();
        db.insert_symbols(file2_id, "test2.py", &file2_symbols, 1, None)
            .unwrap();
        db.insert_symbols(file3_id, "test3.py", &file3_symbols, 1, None)
            .unwrap();

        // Create diffs for batch update
        // File 1: Delete func2, add func3
        let file1_updated = vec![
            make_test_symbol("test1.func1", Some("() -> None"), "function", 10), // unchanged
            make_test_symbol("test1.func3", Some("() -> None"), "function", 30), // added
        ];
        let existing1 = db.get_symbols_for_file("test1.py", 1).unwrap();
        let diff1 = compute_symbol_diff(existing1, file1_updated);

        // File 2: Modify func1 line
        let file2_updated = vec![
            make_test_symbol("test2.func1", Some("() -> None"), "function", 15), // modified
        ];
        let existing2 = db.get_symbols_for_file("test2.py", 1).unwrap();
        let diff2 = compute_symbol_diff(existing2, file2_updated);

        // File 3: No changes (unchanged)
        let file3_updated = file3_symbols.clone();
        let existing3 = db.get_symbols_for_file("test3.py", 1).unwrap();
        let diff3 = compute_symbol_diff(existing3, file3_updated);

        // Create batch
        let batch = vec![
            FileDiff {
                file_id: file1_id,
                file_path: "test1.py".to_string(),
                diff: diff1.clone(),
                graph_version: 1,
                commit_sha: None,
            },
            FileDiff {
                file_id: file2_id,
                file_path: "test2.py".to_string(),
                diff: diff2.clone(),
                graph_version: 1,
                commit_sha: None,
            },
            FileDiff {
                file_id: file3_id,
                file_path: "test3.py".to_string(),
                diff: diff3.clone(),
                graph_version: 1,
                commit_sha: None,
            },
        ];

        // Execute batch update
        let result = db.update_files_symbols_batch(&batch).unwrap();

        // Verify results
        assert_eq!(result.len(), 3, "Should have results for 3 files");
        assert_eq!(result[&file1_id].len(), 2, "File 1 should have 2 symbols");
        assert_eq!(result[&file2_id].len(), 1, "File 2 should have 1 symbol");
        assert_eq!(result[&file3_id].len(), 3, "File 3 should have 3 symbols");

        // Verify file1: func2 deleted, func3 added
        let file1_final = db.get_symbols_for_file("test1.py", 1).unwrap();
        assert_eq!(file1_final.len(), 2);
        assert!(file1_final.iter().any(|s| s.qualname == "test1.func1"));
        assert!(!file1_final.iter().any(|s| s.qualname == "test1.func2")); // deleted
        assert!(file1_final.iter().any(|s| s.qualname == "test1.func3")); // added

        // Verify file2: func1 modified
        let file2_final = db.get_symbols_for_file("test2.py", 1).unwrap();
        assert_eq!(file2_final.len(), 1);
        assert_eq!(file2_final[0].start_line, 15); // line updated

        // Verify file3: unchanged
        let file3_final = db.get_symbols_for_file("test3.py", 1).unwrap();
        assert_eq!(file3_final.len(), 3);
    }

    #[test]
    fn test_batch_vs_individual_correctness() {
        use crate::indexer::batch::FileDiff;
        use crate::indexer::differ::compute_symbol_diff;

        let (mut db, _temp) = create_test_db();

        // Setup: 10 files with symbols
        let num_files = 10;
        let mut file_ids = Vec::new();
        let mut batches = Vec::new();

        for i in 0..num_files {
            let file_path = format!("test{}.py", i);
            let file_id = db
                .upsert_file(&file_path, "hash", "python", 100, 0)
                .unwrap();
            file_ids.push(file_id);

            // Insert initial symbols
            let initial = vec![
                make_test_symbol(
                    &format!("test{}.func1", i),
                    Some("() -> None"),
                    "function",
                    10,
                ),
                make_test_symbol(
                    &format!("test{}.func2", i),
                    Some("() -> None"),
                    "function",
                    20,
                ),
            ];
            db.insert_symbols(file_id, &file_path, &initial, 1, None)
                .unwrap();

            // Create update (modify func1 line, delete func2, add func3)
            let updated = vec![
                make_test_symbol(
                    &format!("test{}.func1", i),
                    Some("() -> None"),
                    "function",
                    15,
                ),
                make_test_symbol(
                    &format!("test{}.func3", i),
                    Some("() -> None"),
                    "function",
                    30,
                ),
            ];

            let existing = db.get_symbols_for_file(&file_path, 1).unwrap();
            let diff = compute_symbol_diff(existing, updated);

            batches.push(FileDiff {
                file_id,
                file_path,
                diff,
                graph_version: 1,
                commit_sha: None,
            });
        }

        // Execute batch update
        let start = std::time::Instant::now();
        db.update_files_symbols_batch(&batches).unwrap();
        let batch_duration = start.elapsed();

        // Verify results
        for i in 0..num_files {
            let file_path = format!("test{}.py", i);
            let symbols = db.get_symbols_for_file(&file_path, 1).unwrap();

            assert_eq!(symbols.len(), 2, "File {} should have 2 symbols", i);
            assert!(
                symbols
                    .iter()
                    .any(|s| s.qualname == format!("test{}.func1", i))
            );
            assert!(
                !symbols
                    .iter()
                    .any(|s| s.qualname == format!("test{}.func2", i))
            ); // deleted
            assert!(
                symbols
                    .iter()
                    .any(|s| s.qualname == format!("test{}.func3", i))
            ); // added

            // Verify func1 line was updated
            let func1 = symbols
                .iter()
                .find(|s| s.qualname == format!("test{}.func1", i))
                .unwrap();
            assert_eq!(func1.start_line, 15);
        }

        println!("Batch update of {} files: {:?}", num_files, batch_duration);
    }

    // ========== CO-CHANGE SUBMODULE TESTS ==========

    fn make_co_change_entry(
        file_a: &str,
        file_b: &str,
        co_change_count: i64,
        confidence: f64,
    ) -> crate::git_mining::CoChangeEntry {
        crate::git_mining::CoChangeEntry {
            file_a: file_a.to_string(),
            file_b: file_b.to_string(),
            co_change_count,
            total_commits_a: co_change_count + 5,
            total_commits_b: co_change_count + 3,
            confidence,
            last_commit_sha: Some("abc123".to_string()),
            last_commit_ts: Some(1700000000),
        }
    }

    #[test]
    fn test_insert_co_changes_batch_empty() {
        let (mut db, _temp) = create_test_db();
        let result = db.insert_co_changes_batch(&[]).unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_insert_and_query_single_co_change() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![make_co_change_entry("src/a.rs", "src/b.rs", 10, 0.8)];
        let count = db.insert_co_changes_batch(&entries).unwrap();
        assert_eq!(count, 1);

        let results = db.co_changes_for_file("src/a.rs", 10, 0.0, 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].file_a, "src/a.rs");
        assert_eq!(results[0].file_b, "src/b.rs");
        assert_eq!(results[0].co_change_count, 10);
        assert!((results[0].confidence - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn test_co_changes_for_file_matches_both_columns() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![make_co_change_entry("src/a.rs", "src/b.rs", 10, 0.8)];
        db.insert_co_changes_batch(&entries).unwrap();

        // Query by file_b — should still find the pair
        let results = db.co_changes_for_file("src/b.rs", 10, 0.0, 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].file_a, "src/a.rs");
        assert_eq!(results[0].file_b, "src/b.rs");
    }

    #[test]
    fn test_co_changes_for_file_respects_min_confidence() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![
            make_co_change_entry("src/a.rs", "src/b.rs", 10, 0.3),
            make_co_change_entry("src/a.rs", "src/c.rs", 5, 0.9),
        ];
        db.insert_co_changes_batch(&entries).unwrap();

        let results = db.co_changes_for_file("src/a.rs", 10, 0.5, 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].file_b, "src/c.rs");
    }

    #[test]
    fn test_co_changes_for_file_respects_limit() {
        let (mut db, _temp) = create_test_db();
        let entries: Vec<_> = (0..20)
            .map(|i| make_co_change_entry("src/a.rs", &format!("src/other_{}.rs", i), 10, 0.5))
            .collect();
        db.insert_co_changes_batch(&entries).unwrap();

        let results = db.co_changes_for_file("src/a.rs", 5, 0.0, 1).unwrap();
        assert_eq!(results.len(), 5);
    }

    #[test]
    fn test_co_changes_for_file_no_match() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![make_co_change_entry("src/a.rs", "src/b.rs", 10, 0.8)];
        db.insert_co_changes_batch(&entries).unwrap();

        let results = db
            .co_changes_for_file("src/nonexistent.rs", 10, 0.0, 1)
            .unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_co_changes_for_file_ordered_by_confidence_desc() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![
            make_co_change_entry("src/a.rs", "src/low.rs", 1, 0.1),
            make_co_change_entry("src/a.rs", "src/high.rs", 20, 0.95),
            make_co_change_entry("src/a.rs", "src/mid.rs", 10, 0.5),
        ];
        db.insert_co_changes_batch(&entries).unwrap();

        let results = db.co_changes_for_file("src/a.rs", 10, 0.0, 1).unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].file_b, "src/high.rs");
        assert_eq!(results[1].file_b, "src/mid.rs");
        assert_eq!(results[2].file_b, "src/low.rs");
    }

    #[test]
    fn test_co_changes_for_files_empty_paths() {
        let (db, _temp) = create_test_db();
        let results = db.co_changes_for_files(&[], 10, 0.0, 1).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_co_changes_for_files_multiple_paths() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![
            make_co_change_entry("src/a.rs", "src/x.rs", 10, 0.8),
            make_co_change_entry("src/b.rs", "src/y.rs", 5, 0.6),
            make_co_change_entry("src/c.rs", "src/z.rs", 3, 0.4),
        ];
        db.insert_co_changes_batch(&entries).unwrap();

        let paths = vec!["src/a.rs".to_string(), "src/b.rs".to_string()];
        let results = db.co_changes_for_files(&paths, 10, 0.0, 1).unwrap();
        assert_eq!(results.len(), 2);
        // Should not include the c.rs/z.rs pair
        for r in &results {
            assert!(r.file_a != "src/c.rs" && r.file_b != "src/z.rs");
        }
    }

    #[test]
    fn test_co_changes_for_files_deduplicates_results() {
        let (mut db, _temp) = create_test_db();
        // Entry where both file_a and file_b are in the query paths
        let entries = vec![make_co_change_entry("src/a.rs", "src/b.rs", 10, 0.8)];
        db.insert_co_changes_batch(&entries).unwrap();

        let paths = vec!["src/a.rs".to_string(), "src/b.rs".to_string()];
        let results = db.co_changes_for_files(&paths, 10, 0.0, 1).unwrap();
        // SQL OR with IN clauses — the row matches both sides, but it's the same row
        // so SQLite returns it once (no DISTINCT needed because it's a single row match)
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_co_changes_for_files_respects_min_confidence() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![
            make_co_change_entry("src/a.rs", "src/x.rs", 10, 0.2),
            make_co_change_entry("src/a.rs", "src/y.rs", 5, 0.7),
        ];
        db.insert_co_changes_batch(&entries).unwrap();

        let paths = vec!["src/a.rs".to_string()];
        let results = db.co_changes_for_files(&paths, 10, 0.5, 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].file_b, "src/y.rs");
    }

    #[test]
    fn test_insert_co_changes_upsert_overwrites() {
        let (mut db, _temp) = create_test_db();

        // Insert initial entry
        let entries = vec![make_co_change_entry("src/a.rs", "src/b.rs", 5, 0.4)];
        db.insert_co_changes_batch(&entries).unwrap();

        // Insert updated entry for the same pair
        let updated = vec![make_co_change_entry("src/a.rs", "src/b.rs", 20, 0.9)];
        db.insert_co_changes_batch(&updated).unwrap();

        let results = db.co_changes_for_file("src/a.rs", 10, 0.0, 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].co_change_count, 20);
        assert!((results[0].confidence - 0.9).abs() < f64::EPSILON);
    }

    #[test]
    fn test_clear_co_changes() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![
            make_co_change_entry("src/a.rs", "src/b.rs", 10, 0.8),
            make_co_change_entry("src/c.rs", "src/d.rs", 5, 0.6),
        ];
        db.insert_co_changes_batch(&entries).unwrap();

        db.clear_co_changes().unwrap();

        let results = db.co_changes_for_file("src/a.rs", 10, 0.0, 1).unwrap();
        assert!(results.is_empty());
        let results = db.co_changes_for_file("src/c.rs", 10, 0.0, 1).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_clear_co_changes_on_empty_table() {
        let (mut db, _temp) = create_test_db();
        // Should not error on empty table
        db.clear_co_changes().unwrap();
    }

    #[test]
    fn test_coupling_hotspots_basic() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![
            make_co_change_entry("src/a.rs", "src/b.rs", 20, 0.95),
            make_co_change_entry("src/c.rs", "src/d.rs", 10, 0.6),
            make_co_change_entry("src/e.rs", "src/f.rs", 3, 0.2),
        ];
        db.insert_co_changes_batch(&entries).unwrap();

        let hotspots = db.coupling_hotspots(10, 0.5).unwrap();
        assert_eq!(hotspots.len(), 2); // excludes 0.2 confidence
        assert_eq!(hotspots[0].file_a, "src/a.rs");
        assert_eq!(hotspots[0].co_change_count, 20);
        assert!((hotspots[0].confidence - 0.95).abs() < f64::EPSILON);
    }

    #[test]
    fn test_coupling_hotspots_respects_limit() {
        let (mut db, _temp) = create_test_db();
        let entries: Vec<_> = (0..10)
            .map(|i| {
                make_co_change_entry(
                    &format!("src/a_{}.rs", i),
                    &format!("src/b_{}.rs", i),
                    10,
                    0.8,
                )
            })
            .collect();
        db.insert_co_changes_batch(&entries).unwrap();

        let hotspots = db.coupling_hotspots(3, 0.0).unwrap();
        assert_eq!(hotspots.len(), 3);
    }

    #[test]
    fn test_coupling_hotspots_empty_table() {
        let (db, _temp) = create_test_db();
        let hotspots = db.coupling_hotspots(10, 0.0).unwrap();
        assert!(hotspots.is_empty());
    }

    #[test]
    fn test_coupling_hotspots_ordered_by_confidence_desc() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![
            make_co_change_entry("src/low.rs", "src/low2.rs", 1, 0.1),
            make_co_change_entry("src/high.rs", "src/high2.rs", 20, 0.99),
            make_co_change_entry("src/mid.rs", "src/mid2.rs", 10, 0.5),
        ];
        db.insert_co_changes_batch(&entries).unwrap();

        let hotspots = db.coupling_hotspots(10, 0.0).unwrap();
        assert_eq!(hotspots.len(), 3);
        assert!((hotspots[0].confidence - 0.99).abs() < f64::EPSILON);
        assert!((hotspots[1].confidence - 0.5).abs() < f64::EPSILON);
        assert!((hotspots[2].confidence - 0.1).abs() < f64::EPSILON);
    }

    #[test]
    fn test_co_change_entry_with_none_fields() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![crate::git_mining::CoChangeEntry {
            file_a: "src/a.rs".to_string(),
            file_b: "src/b.rs".to_string(),
            co_change_count: 5,
            total_commits_a: 10,
            total_commits_b: 8,
            confidence: 0.5,
            last_commit_sha: None,
            last_commit_ts: None,
        }];
        let count = db.insert_co_changes_batch(&entries).unwrap();
        assert_eq!(count, 1);

        let results = db.co_changes_for_file("src/a.rs", 10, 0.0, 1).unwrap();
        assert_eq!(results.len(), 1);
        assert!(results[0].last_commit_sha.is_none());
    }

    #[test]
    fn test_co_change_zero_confidence_boundary() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![
            make_co_change_entry("src/a.rs", "src/b.rs", 1, 0.0),
            make_co_change_entry("src/a.rs", "src/c.rs", 1, 0.001),
        ];
        db.insert_co_changes_batch(&entries).unwrap();

        // min_confidence=0.0 should include the 0.0 entry
        let results = db.co_changes_for_file("src/a.rs", 10, 0.0, 1).unwrap();
        assert_eq!(results.len(), 2);

        // min_confidence just above 0.0 should exclude the 0.0 entry
        let results = db.co_changes_for_file("src/a.rs", 10, 0.0005, 1).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].file_b, "src/c.rs");
    }

    #[test]
    fn test_co_changes_limit_zero() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![make_co_change_entry("src/a.rs", "src/b.rs", 10, 0.8)];
        db.insert_co_changes_batch(&entries).unwrap();

        // limit=0 should return nothing
        let results = db.co_changes_for_file("src/a.rs", 0, 0.0, 1).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_co_changes_for_files_single_path() {
        let (mut db, _temp) = create_test_db();
        let entries = vec![make_co_change_entry("src/a.rs", "src/b.rs", 10, 0.8)];
        db.insert_co_changes_batch(&entries).unwrap();

        let paths = vec!["src/a.rs".to_string()];
        let results = db.co_changes_for_files(&paths, 10, 0.0, 1).unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_insert_co_changes_batch_large_batch() {
        let (mut db, _temp) = create_test_db();
        let entries: Vec<_> = (0..500)
            .map(|i| {
                make_co_change_entry(
                    &format!("src/file_{}.rs", i),
                    &format!("src/file_{}.rs", i + 500),
                    i as i64 + 1,
                    (i as f64) / 500.0,
                )
            })
            .collect();
        let count = db.insert_co_changes_batch(&entries).unwrap();
        assert_eq!(count, 500);

        let hotspots = db.coupling_hotspots(5, 0.0).unwrap();
        assert_eq!(hotspots.len(), 5);
        // Highest confidence should be 499/500 = 0.998
        assert!(hotspots[0].confidence > 0.99);
    }

    // graph_query tests

    #[test]
    fn test_find_symbols_returns_matching_symbols() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            make_test_symbol("my_module.MyClass", Some("struct MyClass"), "class", 1),
            make_test_symbol(
                "my_module.helper_fn",
                Some("fn helper_fn()"),
                "function",
                10,
            ),
        ];
        db.insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        let found = db.find_symbols("MyClass", 10, None, 1).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, "MyClass");

        let found = db.find_symbols("my_module", 10, None, 1).unwrap();
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn test_find_symbols_multi_word_query() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            make_test_symbol("my_module.MyClass", None, "class", 1),
            make_test_symbol("my_module.MyOther", None, "class", 10),
        ];
        db.insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Both tokens must match (AND across tokens)
        let found = db.find_symbols("my_module MyClass", 10, None, 1).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].name, "MyClass");
    }

    #[test]
    fn test_lookup_symbol_id_exact_match() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol("mod.Foo", Some("struct Foo"), "class", 1)];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        let id = db.lookup_symbol_id("mod.Foo", 1).unwrap();
        assert_eq!(id, Some(inserted[0].id));

        let id = db.lookup_symbol_id("mod.Bar", 1).unwrap();
        assert!(id.is_none());
    }

    #[test]
    fn test_lookup_symbol_id_fuzzy_finds_by_suffix() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol(
            "a.b.DeployAsync",
            Some("fn deploy_async()"),
            "method",
            1,
        )];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Fuzzy: short qualname "_svc.DeployAsync" should match by suffix
        let id = db
            .lookup_symbol_id_fuzzy("_svc.DeployAsync", None, 1)
            .unwrap();
        assert_eq!(id, Some(inserted[0].id));
    }

    #[test]
    fn test_edges_for_symbol_returns_edges() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            make_test_symbol("mod.Caller", Some("fn caller()"), "function", 1),
            make_test_symbol("mod.Callee", Some("fn callee()"), "function", 10),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        let edges = vec![crate::indexer::extract::EdgeInput {
            kind: "CALLS".to_string(),
            source_qualname: Some("mod.Caller".to_string()),
            target_qualname: Some("mod.Callee".to_string()),
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: Some(1.0),
            trace_id: None,
            span_id: None,
            event_ts: None,
        }];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let found = db.edges_for_symbol(inserted[0].id, None, 1).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, "CALLS");
    }

    #[test]
    fn test_symbols_by_ids_returns_requested_symbols() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            make_test_symbol("mod.A", None, "class", 1),
            make_test_symbol("mod.B", None, "class", 10),
            make_test_symbol("mod.C", None, "class", 20),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        let ids = vec![inserted[0].id, inserted[2].id];
        let found = db.symbols_by_ids(&ids, None, 1).unwrap();
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].name, "A");
        assert_eq!(found[1].name, "C");
    }

    #[test]
    fn test_symbols_by_ids_empty_input() {
        let (db, _temp) = create_test_db();
        let found = db.symbols_by_ids(&[], None, 1).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_edges_for_symbols_empty_input() {
        let (db, _temp) = create_test_db();
        let found = db.edges_for_symbols(&[], None, 1).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_find_symbols_by_name_prefix() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            make_test_symbol("mod.FooBar", None, "class", 1),
            make_test_symbol("mod.FooBaz", None, "class", 10),
            make_test_symbol("mod.BarQux", None, "class", 20),
        ];
        db.insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        let found = db.find_symbols_by_name_prefix("Foo", 10, None, 1).unwrap();
        assert_eq!(found.len(), 2);
        // All results should start with "Foo"
        for s in &found {
            assert!(s.name.starts_with("Foo"));
        }
    }

    #[test]
    fn test_source_symbols_for_config_uri_empty() {
        let (db, _temp) = create_test_db();
        let found = db
            .source_symbols_for_config_uri("secret://nonexistent", &[], 1)
            .unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_edges_by_target_qualname_and_kinds_empty_kinds() {
        let (db, _temp) = create_test_db();
        let found = db
            .edges_by_target_qualname_and_kinds("some.target", &[], None, 1)
            .unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_incoming_edges_by_qualname_pattern() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            make_test_symbol("mod.Caller", Some("fn caller()"), "function", 1),
            make_test_symbol("mod.Callee", Some("fn callee()"), "function", 10),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        let edges = vec![crate::indexer::extract::EdgeInput {
            kind: "CALLS".to_string(),
            source_qualname: Some("mod.Caller".to_string()),
            target_qualname: Some("mod.Callee".to_string()),
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: Some(1.0),
            trace_id: None,
            span_id: None,
            event_ts: None,
        }];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let found = db
            .incoming_edges_by_qualname_pattern("Callee", "CALLS", None, 1)
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, "CALLS");
    }

    #[test]
    fn test_find_symbols_empty_query_returns_empty() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol("mod.Foo", None, "class", 1)];
        db.insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Empty query should not crash and should return empty results
        let found = db.find_symbols("", 10, None, 1).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_find_symbols_whitespace_query_returns_empty() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol("mod.Foo", None, "class", 1)];
        db.insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Whitespace-only query should not crash and should return empty results
        let found = db.find_symbols("   ", 10, None, 1).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_find_symbols_limit_zero() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol("mod.Foo", None, "class", 1)];
        db.insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        let found = db.find_symbols("Foo", 0, None, 1).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_find_symbols_language_filter() {
        let (mut db, _temp) = create_test_db();
        let file_rs = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let file_py = db
            .upsert_file("src/lib.py", "h2", "python", 100, 0)
            .unwrap();
        let sym_rs = vec![make_test_symbol("mod.Foo", None, "class", 1)];
        let sym_py = vec![make_test_symbol("pkg.Foo", None, "class", 1)];
        db.insert_symbols(file_rs, "src/lib.rs", &sym_rs, 1, None)
            .unwrap();
        db.insert_symbols(file_py, "src/lib.py", &sym_py, 1, None)
            .unwrap();

        // No language filter returns both
        let found = db.find_symbols("Foo", 10, None, 1).unwrap();
        assert_eq!(found.len(), 2);

        // Filter to rust only
        let langs = vec!["rust".to_string()];
        let found = db.find_symbols("Foo", 10, Some(&langs), 1).unwrap();
        assert_eq!(found.len(), 1);
        assert!(found[0].file_path.ends_with(".rs"));

        // Empty languages array behaves like no filter
        let empty_langs: Vec<String> = vec![];
        let found = db.find_symbols("Foo", 10, Some(&empty_langs), 1).unwrap();
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn test_find_symbols_by_name_prefix_empty_prefix() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol("mod.Foo", None, "class", 1)];
        db.insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Empty prefix matches everything (LIKE '%')
        let found = db.find_symbols_by_name_prefix("", 10, None, 1).unwrap();
        assert_eq!(found.len(), 1);
    }

    #[test]
    fn test_find_symbols_by_name_prefix_limit_zero() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol("mod.Foo", None, "class", 1)];
        db.insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        let found = db.find_symbols_by_name_prefix("Foo", 0, None, 1).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_lookup_symbol_id_fuzzy_no_match() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol(
            "a.b.DeployAsync",
            Some("fn deploy_async()"),
            "method",
            1,
        )];
        db.insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Completely unrelated name should return None
        let id = db
            .lookup_symbol_id_fuzzy("_svc.NonexistentMethod", None, 1)
            .unwrap();
        assert!(id.is_none());
    }

    #[test]
    fn test_lookup_symbol_id_fuzzy_exact_name() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol(
            "Deploy",
            Some("fn deploy()"),
            "function",
            1,
        )];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Bare name (no dots) should match via exact name search
        let id = db.lookup_symbol_id_fuzzy("Deploy", None, 1).unwrap();
        assert_eq!(id, Some(inserted[0].id));
    }

    #[test]
    fn test_lookup_symbol_id_fuzzy_multiple_matches_prefers_shortest() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            make_test_symbol("a.b.c.Run", Some("fn run()"), "method", 1),
            make_test_symbol("x.Run", Some("fn run()"), "method", 10),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Should prefer "x.Run" (shorter qualname)
        let id = db.lookup_symbol_id_fuzzy("_svc.Run", None, 1).unwrap();
        assert_eq!(id, Some(inserted[1].id));
    }

    #[test]
    fn test_edges_for_symbol_wrong_graph_version() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            make_test_symbol("mod.Caller", Some("fn caller()"), "function", 1),
            make_test_symbol("mod.Callee", Some("fn callee()"), "function", 10),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        let edges = vec![crate::indexer::extract::EdgeInput {
            kind: "CALLS".to_string(),
            source_qualname: Some("mod.Caller".to_string()),
            target_qualname: Some("mod.Callee".to_string()),
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: Some(1.0),
            trace_id: None,
            span_id: None,
            event_ts: None,
        }];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        // Wrong graph_version returns empty
        let found = db.edges_for_symbol(inserted[0].id, None, 999).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_symbols_by_ids_nonexistent_ids() {
        let (db, _temp) = create_test_db();
        let found = db.symbols_by_ids(&[99999, 88888], None, 1).unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_edges_by_target_qualname_and_kinds_with_data() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            make_test_symbol("mod.Publisher", Some("fn publish()"), "function", 1),
            make_test_symbol("mod.Subscriber", Some("fn subscribe()"), "function", 10),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        let edges = vec![
            crate::indexer::extract::EdgeInput {
                kind: "CHANNEL_PUBLISH".to_string(),
                source_qualname: Some("mod.Publisher".to_string()),
                target_qualname: Some("channel://orders".to_string()),
                detail: None,
                evidence_snippet: None,
                evidence_start_line: None,
                evidence_end_line: None,
                confidence: Some(1.0),
                trace_id: None,
                span_id: None,
                event_ts: None,
            },
            crate::indexer::extract::EdgeInput {
                kind: "CHANNEL_SUBSCRIBE".to_string(),
                source_qualname: Some("mod.Subscriber".to_string()),
                target_qualname: Some("channel://orders".to_string()),
                detail: None,
                evidence_snippet: None,
                evidence_start_line: None,
                evidence_end_line: None,
                confidence: Some(1.0),
                trace_id: None,
                span_id: None,
                event_ts: None,
            },
        ];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        // Search by target qualname with one kind
        let found = db
            .edges_by_target_qualname_and_kinds("channel://orders", &["CHANNEL_PUBLISH"], None, 1)
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, "CHANNEL_PUBLISH");

        // Search by target qualname with both kinds
        let found = db
            .edges_by_target_qualname_and_kinds(
                "channel://orders",
                &["CHANNEL_PUBLISH", "CHANNEL_SUBSCRIBE"],
                None,
                1,
            )
            .unwrap();
        assert_eq!(found.len(), 2);

        // Nonexistent qualname returns empty
        let found = db
            .edges_by_target_qualname_and_kinds(
                "channel://nonexistent",
                &["CHANNEL_PUBLISH"],
                None,
                1,
            )
            .unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_source_symbols_for_config_uri_with_data() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol(
            "mod.ConfigReader",
            Some("fn read_config()"),
            "function",
            1,
        )];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        let edges = vec![crate::indexer::extract::EdgeInput {
            kind: "CONFIG_READ".to_string(),
            source_qualname: Some("mod.ConfigReader".to_string()),
            target_qualname: Some("secret://db-connection".to_string()),
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: Some(1.0),
            trace_id: None,
            span_id: None,
            event_ts: None,
        }];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        // Default kinds (empty) should use CONFIG_SOURCE, CONFIG_READ, CONFIG_BIND
        let found = db
            .source_symbols_for_config_uri("secret://db-connection", &[], 1)
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0], inserted[0].id);

        // Specific kind should also work
        let found = db
            .source_symbols_for_config_uri("secret://db-connection", &["CONFIG_READ"], 1)
            .unwrap();
        assert_eq!(found.len(), 1);

        // Wrong kind returns empty
        let found = db
            .source_symbols_for_config_uri("secret://db-connection", &["CONFIG_SOURCE"], 1)
            .unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_source_symbols_for_config_uri_deduplicates() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol(
            "mod.ConfigReader",
            Some("fn read_config()"),
            "function",
            1,
        )];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Two edges from same symbol to same target
        let edges = vec![
            crate::indexer::extract::EdgeInput {
                kind: "CONFIG_READ".to_string(),
                source_qualname: Some("mod.ConfigReader".to_string()),
                target_qualname: Some("secret://db-conn".to_string()),
                detail: Some("first read".to_string()),
                evidence_snippet: None,
                evidence_start_line: None,
                evidence_end_line: None,
                confidence: Some(1.0),
                trace_id: None,
                span_id: None,
                event_ts: None,
            },
            crate::indexer::extract::EdgeInput {
                kind: "CONFIG_BIND".to_string(),
                source_qualname: Some("mod.ConfigReader".to_string()),
                target_qualname: Some("secret://db-conn".to_string()),
                detail: Some("second bind".to_string()),
                evidence_snippet: None,
                evidence_start_line: None,
                evidence_end_line: None,
                confidence: Some(1.0),
                trace_id: None,
                span_id: None,
                event_ts: None,
            },
        ];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        // Should deduplicate: same source symbol appears once
        let found = db
            .source_symbols_for_config_uri("secret://db-conn", &[], 1)
            .unwrap();
        assert_eq!(found.len(), 1);
    }

    #[test]
    fn test_lookup_symbol_id_filtered_by_language() {
        let (mut db, _temp) = create_test_db();
        let file_id_rs = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let file_id_py = db
            .upsert_file("src/lib.py", "h2", "python", 100, 0)
            .unwrap();

        let sym_rs = vec![make_test_symbol("mod.Foo", None, "class", 1)];
        let sym_py = vec![make_test_symbol("mod.Foo", None, "class", 1)];
        let ins_rs = db
            .insert_symbols(file_id_rs, "src/lib.rs", &sym_rs, 1, None)
            .unwrap();
        let ins_py = db
            .insert_symbols(file_id_py, "src/lib.py", &sym_py, 1, None)
            .unwrap();

        // Without language filter, get any match
        let id = db.lookup_symbol_id("mod.Foo", 1).unwrap();
        assert!(id.is_some());

        // With language filter, get specific match
        let rust_langs = vec!["rust".to_string()];
        let id = db
            .lookup_symbol_id_filtered("mod.Foo", Some(&rust_langs), 1)
            .unwrap();
        assert_eq!(id, Some(ins_rs[0].id));

        let python_langs = vec!["python".to_string()];
        let id = db
            .lookup_symbol_id_filtered("mod.Foo", Some(&python_langs), 1)
            .unwrap();
        assert_eq!(id, Some(ins_py[0].id));
    }
}
