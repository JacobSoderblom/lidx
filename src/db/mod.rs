use crate::config::Config;
use crate::diagnostics::DiagnosticInput;
use crate::indexer::differ::SymbolDiff;
use crate::indexer::extract::{EdgeInput, SymbolInput};
use crate::metrics::{FileMetricsInput, SymbolMetricsInput};
use crate::model::{
    Diagnostic, DiagnosticsSummary, DuplicateGroup, Edge,
    GraphVersion, RepoOverview, Symbol, SymbolComplexity, SymbolCoupling,
};
use anyhow::{Context, Result};
use blake3::Hasher;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, OptionalExtension, Row, params};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod migrations;

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

    pub fn repo_overview(
        &self,
        repo_root: PathBuf,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<RepoOverview> {
        let last_indexed = self.get_meta_i64("last_indexed")?;
        let commit_sha = self.graph_version_commit(graph_version)?;

        let conn = self.read_conn()?;
        let files = count_files_for_version(&conn, languages, graph_version)?;
        let symbols = count_symbols_for_version(&conn, languages, graph_version)?;
        let edges = count_edges_for_version(&conn, languages, graph_version)?;

        Ok(RepoOverview {
            repo_root: repo_root.to_string_lossy().to_string(),
            files,
            symbols,
            edges,
            last_indexed,
            graph_version: Some(graph_version),
            commit_sha,
        })
    }

    pub fn list_languages(&self, graph_version: i64) -> Result<Vec<String>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT language
             FROM files
             WHERE deleted_version IS NULL OR deleted_version > ?
             ORDER BY language",
        )?;
        let rows = stmt.query_map(params![graph_version], |row| row.get(0))?;
        let mut languages = Vec::new();
        for row in rows {
            languages.push(row?);
        }
        Ok(languages)
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

    pub fn list_symbol_refs(&self, graph_version: i64) -> Result<Vec<SymbolRefRecord>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT s.id, s.name, s.qualname, s.kind, f.language
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        )?;
        let rows = stmt.query_map(params![graph_version, graph_version], |row| {
            Ok(SymbolRefRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                qualname: row.get(2)?,
                kind: row.get(3)?,
                language: row.get(4)?,
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

    pub fn symbol_map_for_file(
        &self,
        file_id: i64,
        graph_version: i64,
    ) -> Result<HashMap<String, i64>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, qualname
             FROM symbols
             WHERE file_id = ? AND graph_version = ?",
        )?;
        let rows = stmt.query_map(params![file_id, graph_version], |row| {
            let id: i64 = row.get(0)?;
            let qualname: String = row.get(1)?;
            Ok((qualname, id))
        })?;
        let mut map = HashMap::new();
        for row in rows {
            let (qualname, id) = row?;
            map.insert(qualname, id);
        }
        Ok(map)
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
                    .or_insert_with(Vec::new)
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
                    .or_insert_with(Vec::new)
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
                    .or_insert_with(Vec::new)
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
            let mut fuzzy_lookup_stmt = tx.prepare(
                "SELECT s.id
                 FROM symbols s
                 JOIN files f ON s.file_id = f.id
                 WHERE s.qualname LIKE ?
                   AND s.kind IN ('method', 'function')
                   AND s.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                 ORDER BY LENGTH(s.qualname) ASC
                 LIMIT 1"
            )?;

            for edge in edges {
                let source_id =
                    resolve_symbol_id(&edge.source_qualname, symbol_map, &mut exact_lookup_stmt)?;
                let target_id = resolve_symbol_id(
                    &edge.target_qualname,
                    symbol_map,
                    &mut exact_lookup_stmt,
                )?.or_else(|| {
                    // Fuzzy fallback: try suffix match
                    edge.target_qualname.as_ref().and_then(|qn| {
                        let method_name = qn.split('.').last().unwrap_or(qn);
                        let pattern = format!("%.{}", method_name);
                        fuzzy_lookup_stmt
                            .query_row(params![&pattern, graph_version, graph_version], |row| row.get(0))
                            .optional()
                            .ok()
                            .flatten()
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

            // Find batch of unresolved edges
            let unresolved: Vec<(i64, String)> = {
                let mut stmt = tx.prepare(
                    "SELECT id, target_qualname
                     FROM edges
                     WHERE target_symbol_id IS NULL
                     AND target_qualname IS NOT NULL
                     AND graph_version = ?
                     LIMIT ?"
                )?;
                let rows = stmt.query_map(params![graph_version, BATCH_SIZE], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })?;
                rows.collect::<Result<Vec<_>, _>>()?
            };

            if unresolved.is_empty() {
                break;
            }

            let mut count = 0;
            {
                let mut fuzzy_stmt = tx.prepare(
                    "SELECT s.id
                     FROM symbols s
                     JOIN files f ON s.file_id = f.id
                     WHERE s.qualname LIKE ?
                       AND s.kind IN ('method', 'function')
                       AND s.graph_version = ?
                       AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                     ORDER BY LENGTH(s.qualname) ASC
                     LIMIT 1"
                )?;

                let mut update_stmt = tx.prepare(
                    "UPDATE edges SET target_symbol_id = ? WHERE id = ?"
                )?;

                for (edge_id, target_qualname) in &unresolved {
                    let method_name = target_qualname.split('.').last().unwrap_or(target_qualname);
                    let pattern = format!("%.{}", method_name);

                    if let Some(symbol_id) = fuzzy_stmt
                        .query_row(params![&pattern, graph_version, graph_version], |row| row.get::<_, i64>(0))
                        .optional()?
                    {
                        update_stmt.execute(params![symbol_id, edge_id])?;
                        count += 1;
                    }
                }
            } // fuzzy_stmt and update_stmt dropped here

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

    pub fn insert_diagnostics(&mut self, diagnostics: &[DiagnosticInput]) -> Result<usize> {
        if diagnostics.is_empty() {
            return Ok(0);
        }
        let created = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut count = 0;
        {
            let mut insert = tx.prepare(
                "INSERT INTO diagnostics
                 (file_id, path, line, column, end_line, end_column, severity, message, rule_id, tool, snippet, diagnostic_hash, created)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(diagnostic_hash) DO NOTHING",
            )?;
            let mut lookup = tx.prepare("SELECT id FROM files WHERE path = ? LIMIT 1")?;
            for diagnostic in diagnostics {
                let file_id = if let Some(path) = diagnostic.path.as_deref() {
                    lookup
                        .query_row(params![path], |row| row.get::<_, i64>(0))
                        .optional()?
                } else {
                    None
                };
                let hash = diagnostic.fingerprint();
                let inserted = insert.execute(params![
                    file_id,
                    diagnostic.path.as_deref(),
                    diagnostic.line,
                    diagnostic.column,
                    diagnostic.end_line,
                    diagnostic.end_column,
                    diagnostic.severity.as_deref(),
                    diagnostic.message,
                    diagnostic.rule_id.as_deref(),
                    diagnostic.tool.as_deref(),
                    diagnostic.snippet.as_deref(),
                    hash,
                    created,
                ])?;
                count += inserted;
            }
        }
        tx.commit()?;
        Ok(count)
    }

    pub fn find_symbols(
        &self,
        query: &str,
        limit: usize,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Symbol>> {
        let pattern = format!("%{}%", query);
        let query_lower = query.to_lowercase();
        let limit = limit as i64;
        let mut sql = String::from(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE (s.name LIKE ? OR s.qualname LIKE ?)
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> =
            vec![&pattern, &pattern, &graph_version, &graph_version];
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        // Relevance-based ordering:
        // 1. Exact name match first
        // 2. Code symbols before doc/heading symbols
        // 3. Demote changelog/migration files
        // 4. Shorter qualnames (less nesting) first
        sql.push_str(
            " ORDER BY \
             CASE WHEN LOWER(s.name) = ? THEN 0 ELSE 1 END, \
             CASE WHEN s.kind IN ('class','function','method','struct','interface','enum','trait','service') THEN 0 \
                  WHEN s.kind IN ('module','namespace','package') THEN 1 \
                  WHEN s.kind IN ('heading','section') THEN 3 \
                  ELSE 2 END, \
             CASE WHEN f.path LIKE '%changelog%' OR f.path LIKE '%migration%' THEN 1 ELSE 0 END, \
             LENGTH(s.qualname), \
             s.name \
             LIMIT ?",
        );
        params.push(&query_lower);
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| symbol_from_row(row))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Search symbols where name starts with the given prefix.
    /// Used for fuzzy matching candidate retrieval.
    pub fn find_symbols_by_name_prefix(
        &self,
        prefix: &str,
        limit: usize,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Symbol>> {
        let pattern = format!("{}%", prefix);
        let limit = limit as i64;
        let mut sql = String::from(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.name LIKE ?
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> =
            vec![&pattern, &graph_version, &graph_version];
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        sql.push_str(" ORDER BY s.name LIMIT ?");
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| symbol_from_row(row))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
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
                |row| symbol_from_row(row),
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
                |row| symbol_from_row(row),
            )
            .optional()
            .map_err(Into::into)
    }

    /// Get all symbols for a file by path
    ///
    /// This is used during embedding integration to get symbols after they've been inserted
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
                |row| symbol_from_row(row),
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
                |row| symbol_from_row(row),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn lookup_symbol_id(&self, qualname: &str, graph_version: i64) -> Result<Option<i64>> {
        self.lookup_symbol_id_filtered(qualname, None, graph_version)
    }

    pub fn lookup_symbol_id_filtered(
        &self,
        qualname: &str,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Option<i64>> {
        let mut sql = String::from(
            "SELECT s.id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.qualname = ?
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&qualname, &graph_version, &graph_version];
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        sql.push_str(" LIMIT 1");
        self.read_conn()?
            .query_row(&sql, &*params, |row| row.get(0))
            .optional()
            .map_err(Into::into)
    }

    /// Fuzzy lookup for symbol IDs, handling short qualnames like "_svc.DeployAsync"
    ///
    /// Strategy:
    /// 1. Try exact match first (fast path)
    /// 2. Extract method/function name from short qualname (part after last '.')
    /// 3. Search for symbols whose qualname ends with '.{name}'
    /// 4. Prefer shortest qualname match (less nesting = more specific)
    pub fn lookup_symbol_id_fuzzy(
        &self,
        target_qualname: &str,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Option<i64>> {
        // Fast path: try exact match first
        if let Some(id) = self.lookup_symbol_id_filtered(target_qualname, languages, graph_version)? {
            return Ok(Some(id));
        }

        // Extract the method/function name from the short qualname
        let name = target_qualname.split('.').last().unwrap_or(target_qualname);

        // Search for symbols whose qualname ends with '.{name}'
        let pattern = format!("%.{}", name);

        let mut sql = String::from(
            "SELECT s.id, s.qualname, LENGTH(s.qualname) as qn_len
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.qualname LIKE ?
               AND s.kind IN ('method', 'function')
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );

        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&pattern, &graph_version, &graph_version];

        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }

        sql.push_str(" ORDER BY qn_len ASC LIMIT 10");

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query(&*params)?;

        let mut candidates: Vec<(i64, String)> = Vec::new();
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let qualname: String = row.get(1)?;
            candidates.push((id, qualname));
        }

        // If exactly 1 match, return it
        if candidates.len() == 1 {
            return Ok(Some(candidates[0].0));
        }

        // If multiple matches, prefer the shortest qualname (already ordered by qn_len)
        if !candidates.is_empty() {
            return Ok(Some(candidates[0].0));
        }

        Ok(None)
    }

    pub fn edges_for_symbol(
        &self,
        id: i64,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        let mut sql = String::from(
            "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                    e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE (e.source_symbol_id = ? OR e.target_symbol_id = ?)
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&id, &id, &graph_version, &graph_version];
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        sql.push_str(" ORDER BY e.id");
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| edge_from_row(row))?;
        let mut edges = Vec::new();
        for row in rows {
            edges.push(row?);
        }
        Ok(edges)
    }

    /// Find incoming edges by target_qualname pattern
    /// Used for finding callers when target_symbol_id is null but target_qualname is set
    pub fn incoming_edges_by_qualname_pattern(
        &self,
        symbol_name: &str,
        kind: &str,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        // Search for edges where target_qualname ends with '.<symbol_name>'
        let pattern = format!("%.{}", symbol_name);

        let mut sql = String::from(
            "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                    e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE e.target_qualname LIKE ?
               AND e.kind = ?
               AND e.source_symbol_id IS NOT NULL
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );

        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&pattern, &kind, &graph_version, &graph_version];

        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }

        sql.push_str(" ORDER BY e.id LIMIT 100");

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| edge_from_row(row))?;
        let mut edges = Vec::new();
        for row in rows {
            edges.push(row?);
        }
        Ok(edges)
    }

    /// Find edges by exact target_qualname match and edge kind filter.
    /// Used for traversal bridging: given a channel/route qualname, find all
    /// edges pointing at it with complementary kinds.
    pub fn edges_by_target_qualname_and_kinds(
        &self,
        target_qualname: &str,
        kinds: &[&str],
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        if kinds.is_empty() {
            return Ok(Vec::new());
        }
        let mut kind_placeholders = String::new();
        for (idx, _) in kinds.iter().enumerate() {
            if idx > 0 {
                kind_placeholders.push(',');
            }
            kind_placeholders.push('?');
        }
        let mut sql = format!(
            "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                    e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE e.target_qualname = ?
               AND e.kind IN ({kind_placeholders})
               AND e.source_symbol_id IS NOT NULL
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)"
        );
        let mut params: Vec<&dyn rusqlite::ToSql> =
            vec![&target_qualname as &dyn rusqlite::ToSql];
        for kind in kinds {
            params.push(kind as &dyn rusqlite::ToSql);
        }
        params.push(&graph_version);
        params.push(&graph_version);
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        sql.push_str(" ORDER BY e.id LIMIT 100");
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| edge_from_row(row))?;
        let mut edges = Vec::new();
        for row in rows {
            edges.push(row?);
        }
        Ok(edges)
    }

    pub fn edges_for_symbols(
        &self,
        ids: &[i64],
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<HashMap<i64, Vec<Edge>>> {
        if ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut placeholders = String::new();
        for (idx, _) in ids.iter().enumerate() {
            if idx > 0 {
                placeholders.push(',');
            }
            placeholders.push('?');
        }

        // First query: resolved edges (existing behavior)
        let mut sql = format!(
            "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                    e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE (e.source_symbol_id IN ({}) OR e.target_symbol_id IN ({}))
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
            placeholders, placeholders
        );

        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
        // Add IDs twice (for source and target)
        for id in ids {
            params.push(id as &dyn rusqlite::ToSql);
        }
        for id in ids {
            params.push(id as &dyn rusqlite::ToSql);
        }
        params.push(&graph_version);
        params.push(&graph_version);

        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        sql.push_str(" ORDER BY e.id");

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| edge_from_row(row))?;

        // Group edges by symbol ID
        let mut result: HashMap<i64, Vec<Edge>> = HashMap::new();
        for id in ids {
            result.insert(*id, Vec::new());
        }

        let mut seen_edge_ids = std::collections::HashSet::new();
        for row in rows {
            let edge = row?;
            seen_edge_ids.insert(edge.id);
            // Add edge to both source and target symbol lists
            if let Some(source_id) = edge.source_symbol_id {
                if ids.contains(&source_id) {
                    result.entry(source_id).or_default().push(edge.clone());
                }
            }
            if let Some(target_id) = edge.target_symbol_id {
                if ids.contains(&target_id) {
                    result.entry(target_id).or_default().push(edge.clone());
                }
            }
        }

        // Second query: unresolved edges where target_qualname matches symbol names
        // This catches cross-file CALLS edges with short qualnames like "_svc.DeployAsync"
        let symbols_sql = format!(
            "SELECT id, name FROM symbols WHERE id IN ({}) AND graph_version = ?",
            placeholders
        );
        let mut symbols_params: Vec<&dyn rusqlite::ToSql> = ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
        symbols_params.push(&graph_version);

        let mut stmt = conn.prepare(&symbols_sql)?;
        let mut symbol_rows = stmt.query(&*symbols_params)?;
        let mut symbol_names: std::collections::HashMap<String, i64> = std::collections::HashMap::new();
        while let Some(row) = symbol_rows.next()? {
            let id: i64 = row.get(0)?;
            let name: String = row.get(1)?;
            symbol_names.insert(name, id);
        }

        // Build patterns for LIKE queries: %.MethodName
        let mut patterns: Vec<String> = Vec::new();
        for name in symbol_names.keys() {
            patterns.push(format!("%.{}", name));
        }

        if !patterns.is_empty() {
            let mut unresolved_sql = String::from(
                "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                        e.target_qualname, e.detail, e.evidence_snippet,
                        e.evidence_start_line, e.evidence_end_line, e.confidence,
                        e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts
                 FROM edges e
                 JOIN files f ON e.file_id = f.id
                 WHERE e.target_symbol_id IS NULL
                   AND e.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                   AND (",
            );

            let mut unresolved_params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];

            for (idx, pattern) in patterns.iter().enumerate() {
                if idx > 0 {
                    unresolved_sql.push_str(" OR ");
                }
                unresolved_sql.push_str("e.target_qualname LIKE ?");
                unresolved_params.push(pattern as &dyn rusqlite::ToSql);
            }
            unresolved_sql.push(')');

            if let Some(languages) = languages {
                if !languages.is_empty() {
                    unresolved_sql.push_str(" AND f.language IN (");
                    for (idx, _) in languages.iter().enumerate() {
                        if idx > 0 {
                            unresolved_sql.push(',');
                        }
                        unresolved_sql.push('?');
                    }
                    unresolved_sql.push(')');
                    for language in languages {
                        unresolved_params.push(language as &dyn rusqlite::ToSql);
                    }
                }
            }
            unresolved_sql.push_str(" ORDER BY e.id");

            let mut stmt = conn.prepare(&unresolved_sql)?;
            let rows = stmt.query_map(&*unresolved_params, |row| edge_from_row(row))?;

            for row in rows {
                let edge = row?;
                // Skip if we already saw this edge in the first query
                if seen_edge_ids.contains(&edge.id) {
                    continue;
                }

                // Match target_qualname to symbol name and add to that symbol's edge list
                if let Some(target_qn) = &edge.target_qualname {
                    for (name, symbol_id) in &symbol_names {
                        if target_qn.ends_with(&format!(".{}", name)) {
                            result.entry(*symbol_id).or_default().push(edge.clone());
                            break;
                        }
                    }
                }
            }
        }

        Ok(result)
    }

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
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        let rows = stmt.query_map(&*params, |row| edge_from_row(row))?;
        let mut edges = Vec::new();
        for row in rows {
            edges.push(row?);
        }
        Ok(edges)
    }

    pub fn symbols_by_ids(
        &self,
        ids: &[i64],
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Symbol>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut placeholders = String::new();
        for (idx, _) in ids.iter().enumerate() {
            if idx > 0 {
                placeholders.push(',');
            }
            placeholders.push('?');
        }
        let mut sql = format!(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.id IN ({})
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
            placeholders
        );
        let mut params: Vec<&dyn rusqlite::ToSql> =
            ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
        params.push(&graph_version);
        params.push(&graph_version);
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        sql.push_str(" ORDER BY s.id");
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| symbol_from_row(row))?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn call_edge_count(
        &self,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<i64> {
        let mut sql = String::from(
            "SELECT COUNT(*)
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE e.kind = 'CALLS'
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");
        let conn = self.read_conn()?;
        let count: i64 = if params.is_empty() {
            conn.query_row(&sql, [], |row| row.get(0))?
        } else {
            conn.query_row(&sql, &*params, |row| row.get(0))?
        };
        Ok(count)
    }

    pub fn top_complexity(
        &self,
        limit: usize,
        min_complexity: i64,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<SymbolComplexity>> {
        let mut sql = String::from(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id,
                    sm.loc, sm.complexity
             FROM symbol_metrics sm
             JOIN symbols s ON sm.symbol_id = s.id
             JOIN files f ON sm.file_id = f.id
             WHERE sm.complexity >= ?
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> =
            vec![&min_complexity, &graph_version, &graph_version];
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");
        sql.push_str(" ORDER BY sm.complexity DESC, sm.loc DESC, s.id");
        sql.push_str(" LIMIT ?");
        let limit = limit as i64;
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| {
            let symbol = symbol_from_row(row)?;
            let loc: i64 = row.get(16)?;
            let complexity: i64 = row.get(17)?;
            Ok(SymbolComplexity {
                symbol,
                loc,
                complexity,
            })
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn top_fan_in(
        &self,
        limit: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<SymbolCoupling>> {
        let mut sql = String::from(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id,
                    COUNT(*) as fan_in
             FROM edges e
             JOIN symbols s ON e.target_symbol_id = s.id
             JOIN files f ON s.file_id = f.id
             WHERE e.kind = 'CALLS'
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");
        sql.push_str(" GROUP BY e.target_symbol_id");
        sql.push_str(" ORDER BY fan_in DESC, s.id");
        sql.push_str(" LIMIT ?");
        let limit = limit as i64;
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| {
            let symbol = symbol_from_row(row)?;
            let count: i64 = row.get(16)?;
            Ok(SymbolCoupling { symbol, count })
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn top_fan_out(
        &self,
        limit: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<SymbolCoupling>> {
        let mut sql = String::from(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id,
                    COUNT(*) as fan_out
             FROM edges e
             JOIN symbols s ON e.source_symbol_id = s.id
             JOIN files f ON s.file_id = f.id
             WHERE e.kind = 'CALLS'
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");
        sql.push_str(" GROUP BY e.source_symbol_id");
        sql.push_str(" ORDER BY fan_out DESC, s.id");
        sql.push_str(" LIMIT ?");
        let limit = limit as i64;
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| {
            let symbol = symbol_from_row(row)?;
            let count: i64 = row.get(16)?;
            Ok(SymbolCoupling { symbol, count })
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn top_fan_in_by_module(
        &self,
        limit_per_module: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<(String, Symbol, i64)>> {
        let mut sql = String::from(
            "SELECT
                CASE
                    WHEN INSTR(f.path, '/') > 0
                    THEN SUBSTR(f.path, 1, INSTR(f.path, '/') - 1)
                    ELSE f.path
                END as module,
                s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                s.graph_version, s.commit_sha, s.stable_id,
                COUNT(e.id) as fan_in
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             LEFT JOIN edges e ON e.target_symbol_id = s.id AND e.kind = 'CALLS' AND e.graph_version = ?
             WHERE s.graph_version = ?
               AND s.kind IN ('function','method','class','struct','interface','service')
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version, &graph_version];

        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }

        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");
        sql.push_str(" GROUP BY s.id");
        sql.push_str(" HAVING fan_in > 0");
        sql.push_str(" ORDER BY module, fan_in DESC");

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| {
            let module: String = row.get(0)?;
            let symbol = symbol_from_row_offset(row, 1)?;
            let fan_in: i64 = row.get(17)?;
            Ok((module, symbol, fan_in))
        })?;

        // Collect and group by module, limiting per module
        let mut by_module: std::collections::HashMap<String, Vec<(Symbol, i64)>> = std::collections::HashMap::new();
        for row in rows {
            let (module, symbol, fan_in) = row?;
            by_module.entry(module).or_default().push((symbol, fan_in));
        }

        // Flatten with limit per module
        let mut results = Vec::new();
        for (module, mut symbols) in by_module {
            symbols.truncate(limit_per_module);
            for (symbol, fan_in) in symbols {
                results.push((module.clone(), symbol, fan_in));
            }
        }

        Ok(results)
    }

    pub fn count_symbols_by_kind(
        &self,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<(String, i64)>> {
        let mut sql = String::from(
            "SELECT s.kind, COUNT(*) as cnt
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];

        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }

        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");
        sql.push_str(" GROUP BY s.kind ORDER BY cnt DESC");

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| {
            let kind: String = row.get(0)?;
            let cnt: i64 = row.get(1)?;
            Ok((kind, cnt))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn duplicate_groups(
        &self,
        limit: usize,
        min_count: i64,
        min_loc: i64,
        per_group_limit: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<DuplicateGroup>> {
        let mut sql = String::from(
            "SELECT sm.duplication_hash, COUNT(*) as count
             FROM symbol_metrics sm
             JOIN symbols s ON sm.symbol_id = s.id
             JOIN files f ON sm.file_id = f.id
             WHERE sm.duplication_hash IS NOT NULL AND sm.loc >= ?
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&min_loc, &graph_version, &graph_version];
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }
        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");
        sql.push_str(" GROUP BY sm.duplication_hash HAVING COUNT(*) >= ?");
        params.push(&min_count);
        sql.push_str(" ORDER BY count DESC, sm.duplication_hash");
        sql.push_str(" LIMIT ?");
        let limit = limit as i64;
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let groups = stmt.query_map(&*params, |row| {
            let hash: String = row.get(0)?;
            let count: i64 = row.get(1)?;
            Ok((hash, count))
        })?;

        let mut results = Vec::new();
        for row in groups {
            let (hash, count) = row?;
            let mut member_sql = String::from(
                "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                        s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                        s.graph_version, s.commit_sha, s.stable_id
                 FROM symbol_metrics sm
                 JOIN symbols s ON sm.symbol_id = s.id
                 JOIN files f ON sm.file_id = f.id
                 WHERE sm.duplication_hash = ? AND sm.loc >= ?
                   AND s.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
            );
            let mut member_params: Vec<&dyn rusqlite::ToSql> =
                vec![&hash, &min_loc, &graph_version, &graph_version];
            if let Some(languages) = languages {
                if !languages.is_empty() {
                    member_sql.push_str(" AND f.language IN (");
                    for (idx, _) in languages.iter().enumerate() {
                        if idx > 0 {
                            member_sql.push(',');
                        }
                        member_sql.push('?');
                    }
                    member_sql.push(')');
                    for language in languages {
                        member_params.push(language as &dyn rusqlite::ToSql);
                    }
                }
            }
            let mut member_path_params = Vec::new();
            append_path_filters(
                &mut member_sql,
                &mut member_params,
                &mut member_path_params,
                paths,
                "f",
            );
            member_sql.push_str(" ORDER BY f.path, s.start_line, s.id LIMIT ?");
            let per_group_limit = per_group_limit as i64;
            member_params.push(&per_group_limit);
            let member_conn = self.read_conn()?;
            let mut member_stmt = member_conn.prepare(&member_sql)?;
            let rows = member_stmt.query_map(&*member_params, |row| symbol_from_row(row))?;
            let mut symbols = Vec::new();
            for row in rows {
                symbols.push(row?);
            }
            results.push(DuplicateGroup {
                hash,
                count,
                symbols,
            });
        }
        Ok(results)
    }

    pub fn dead_symbols(
        &self,
        limit: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Symbol>> {
        let sql = "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                          s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                          s.graph_version, s.commit_sha, s.stable_id
                   FROM symbols s
                   JOIN files f ON s.file_id = f.id
                   WHERE s.graph_version = ?
                     AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                     AND s.kind IN ('function', 'method', 'class', 'struct')
                     AND s.name NOT IN ('main', '__init__', 'setup', 'teardown', 'configure', 'register')
                     AND NOT EXISTS (
                       SELECT 1 FROM edges e
                       WHERE e.target_symbol_id = s.id
                         AND e.kind IN ('CALLS', 'IMPORTS', 'RPC_IMPL', 'IMPLEMENTS', 'EXTENDS')
                         AND e.graph_version = ?
                     )
                     AND NOT EXISTS (
                       SELECT 1 FROM edges e
                       WHERE e.target_symbol_id = s.id
                         AND e.kind = 'IMPORTS'
                         AND e.graph_version = ?
                         AND e.file_id != s.file_id
                     )";

        let mut full_sql = String::from(sql);
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![
            &graph_version,
            &graph_version,
            &graph_version,
            &graph_version,
        ];

        if let Some(languages) = languages {
            if !languages.is_empty() {
                full_sql.push_str(" AND f.language IN (");
                for (idx, _) in languages.iter().enumerate() {
                    if idx > 0 {
                        full_sql.push(',');
                    }
                    full_sql.push('?');
                }
                full_sql.push(')');
                for language in languages {
                    params.push(language as &dyn rusqlite::ToSql);
                }
            }
        }

        let mut path_params = Vec::new();
        append_path_filters(&mut full_sql, &mut params, &mut path_params, paths, "f");

        full_sql.push_str(" ORDER BY s.qualname LIMIT ?");
        let limit = limit as i64;
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&full_sql)?;
        let rows = stmt.query_map(&*params, |row| symbol_from_row(row))?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn unused_imports(
        &self,
        limit: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        let sql = "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                          e.target_qualname, e.detail, e.evidence_snippet,
                          e.evidence_start_line, e.evidence_end_line, e.confidence,
                          e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts
                   FROM edges e
                   JOIN files f ON e.file_id = f.id
                   WHERE e.kind = 'IMPORTS'
                     AND e.graph_version = ?
                     AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                     AND e.target_qualname IS NOT NULL
                     AND NOT EXISTS (
                       SELECT 1 FROM edges e2
                       WHERE e2.kind = 'CALLS'
                         AND e2.file_id = e.file_id
                         AND e2.target_qualname = e.target_qualname
                         AND e2.graph_version = ?
                     )";

        let mut full_sql = String::from(sql);
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![
            &graph_version,
            &graph_version,
            &graph_version,
        ];

        if let Some(languages) = languages {
            if !languages.is_empty() {
                full_sql.push_str(" AND f.language IN (");
                for (idx, _) in languages.iter().enumerate() {
                    if idx > 0 {
                        full_sql.push(',');
                    }
                    full_sql.push('?');
                }
                full_sql.push(')');
                for language in languages {
                    params.push(language as &dyn rusqlite::ToSql);
                }
            }
        }

        let mut path_params = Vec::new();
        append_path_filters(&mut full_sql, &mut params, &mut path_params, paths, "f");

        full_sql.push_str(" ORDER BY f.path, e.evidence_start_line LIMIT ?");
        let limit = limit as i64;
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&full_sql)?;
        let rows = stmt.query_map(&*params, |row| edge_from_row(row))?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn orphan_tests(
        &self,
        limit: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Symbol>> {
        // First, get all test symbols
        let mut sql = String::from(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
               AND s.kind IN ('function', 'method')
               AND (s.name LIKE 'test_%' OR s.name LIKE 'Test%'
                    OR f.path LIKE '%test%' OR f.path LIKE '%spec%')",
        );

        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];

        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }

        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");

        // Cap scan to limit*10 to avoid unbounded N+1 queries
        let scan_cap = limit * 10;
        sql.push_str(&format!(" ORDER BY s.qualname LIMIT {}", scan_cap));

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| symbol_from_row(row))?;
        let mut all_tests = Vec::new();
        for row in rows {
            all_tests.push(row?);
        }

        // Extract target names for all test symbols in batch
        let mut tests_with_targets: Vec<(Symbol, String)> = Vec::new();
        for test_symbol in all_tests {
            let target_name = extract_target_name(&test_symbol.name);
            if !target_name.is_empty() {
                tests_with_targets.push((test_symbol, target_name));
            }
        }

        if tests_with_targets.is_empty() {
            return Ok(Vec::new());
        }

        // Batch query: get all symbol names that exist in the project
        let unique_targets: Vec<&str> = tests_with_targets
            .iter()
            .map(|(_, t)| t.as_str())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let mut existing_targets = std::collections::HashSet::new();
        for chunk in unique_targets.chunks(500) {
            let placeholders = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let batch_sql = format!(
                "SELECT DISTINCT s.name FROM symbols s
                 JOIN files f ON s.file_id = f.id
                 WHERE s.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                   AND s.name IN ({})",
                placeholders
            );
            let mut batch_params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];
            for name in chunk {
                batch_params.push(name as &dyn rusqlite::ToSql);
            }
            let mut batch_stmt = conn.prepare(&batch_sql)?;
            let rows = batch_stmt.query_map(&*batch_params, |row| row.get::<_, String>(0))?;
            for row in rows {
                existing_targets.insert(row?);
            }
        }

        // Filter to orphan tests (target name not found)
        let mut orphans = Vec::new();
        for (test_symbol, target_name) in tests_with_targets {
            if !existing_targets.contains(&target_name) {
                orphans.push(test_symbol);
                if orphans.len() >= limit {
                    break;
                }
            }
        }

        Ok(orphans)
    }

    pub fn list_diagnostics(
        &self,
        limit: usize,
        offset: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        severity: Option<&String>,
        rule_id: Option<&String>,
        tool: Option<&String>,
    ) -> Result<Vec<Diagnostic>> {
        let mut sql = String::from(
            "SELECT d.id, d.path, d.line, d.column, d.end_line, d.end_column, d.severity,
                    d.message, d.rule_id, d.tool, d.snippet
             FROM diagnostics d
             LEFT JOIN files f ON d.file_id = f.id",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
        let mut path_params = Vec::new();
        append_diagnostics_filters(
            &mut sql,
            &mut params,
            &mut path_params,
            languages,
            paths,
            severity,
            rule_id,
            tool,
        );
        sql.push_str(" ORDER BY d.id DESC LIMIT ? OFFSET ?");
        let limit = limit as i64;
        let offset = offset as i64;
        params.push(&limit);
        params.push(&offset);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| diagnostic_from_row(row))?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn diagnostics_summary(
        &self,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        severity: Option<&String>,
        rule_id: Option<&String>,
        tool: Option<&String>,
    ) -> Result<DiagnosticsSummary> {
        let mut total_sql = String::from(
            "SELECT COUNT(*)
             FROM diagnostics d
             LEFT JOIN files f ON d.file_id = f.id",
        );
        let mut total_params: Vec<&dyn rusqlite::ToSql> = Vec::new();
        let mut total_path_params = Vec::new();
        append_diagnostics_filters(
            &mut total_sql,
            &mut total_params,
            &mut total_path_params,
            languages,
            paths,
            severity,
            rule_id,
            tool,
        );
        let conn = self.read_conn()?;
        let total: i64 = if total_params.is_empty() {
            conn.query_row(&total_sql, [], |row| row.get(0))?
        } else {
            conn.query_row(&total_sql, &*total_params, |row| row.get(0))?
        };

        let mut severity_sql = String::from(
            "SELECT COALESCE(d.severity, '') as key, COUNT(*) as count
             FROM diagnostics d
             LEFT JOIN files f ON d.file_id = f.id",
        );
        let mut severity_params: Vec<&dyn rusqlite::ToSql> = Vec::new();
        let mut severity_path_params = Vec::new();
        append_diagnostics_filters(
            &mut severity_sql,
            &mut severity_params,
            &mut severity_path_params,
            languages,
            paths,
            severity,
            rule_id,
            tool,
        );
        severity_sql.push_str(" GROUP BY key ORDER BY count DESC, key");
        let mut by_severity = BTreeMap::new();
        let severity_conn = self.read_conn()?;
        let mut stmt = severity_conn.prepare(&severity_sql)?;
        let rows = stmt.query_map(&*severity_params, |row| {
            let key: String = row.get(0)?;
            let count: i64 = row.get(1)?;
            Ok((key, count))
        })?;
        for row in rows {
            let (key, count) = row?;
            let label = if key.is_empty() {
                "unknown".to_string()
            } else {
                key
            };
            by_severity.insert(label, count);
        }

        let mut tool_sql = String::from(
            "SELECT COALESCE(d.tool, '') as key, COUNT(*) as count
             FROM diagnostics d
             LEFT JOIN files f ON d.file_id = f.id",
        );
        let mut tool_params: Vec<&dyn rusqlite::ToSql> = Vec::new();
        let mut tool_path_params = Vec::new();
        append_diagnostics_filters(
            &mut tool_sql,
            &mut tool_params,
            &mut tool_path_params,
            languages,
            paths,
            severity,
            rule_id,
            tool,
        );
        tool_sql.push_str(" GROUP BY key ORDER BY count DESC, key");
        let mut by_tool = BTreeMap::new();
        let tool_conn = self.read_conn()?;
        let mut stmt = tool_conn.prepare(&tool_sql)?;
        let rows = stmt.query_map(&*tool_params, |row| {
            let key: String = row.get(0)?;
            let count: i64 = row.get(1)?;
            Ok((key, count))
        })?;
        for row in rows {
            let (key, count) = row?;
            let label = if key.is_empty() {
                "unknown".to_string()
            } else {
                key
            };
            by_tool.insert(label, count);
        }

        Ok(DiagnosticsSummary {
            total,
            by_severity,
            by_tool,
        })
    }

    // Co-change methods for git mining intelligence

    pub fn insert_co_changes_batch(
        &mut self,
        entries: &[crate::git_mining::CoChangeEntry],
    ) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }

        let mined_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut count = 0;

        {
            let mut insert = tx.prepare(
                "INSERT INTO co_changes
                 (file_a, file_b, co_change_count, total_commits_a, total_commits_b, confidence, last_commit_sha, last_commit_ts, mined_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(file_a, file_b) DO UPDATE SET
                   co_change_count = excluded.co_change_count,
                   total_commits_a = excluded.total_commits_a,
                   total_commits_b = excluded.total_commits_b,
                   confidence = excluded.confidence,
                   last_commit_sha = excluded.last_commit_sha,
                   last_commit_ts = excluded.last_commit_ts,
                   mined_at = excluded.mined_at",
            )?;

            for entry in entries {
                insert.execute(params![
                    entry.file_a,
                    entry.file_b,
                    entry.co_change_count,
                    entry.total_commits_a,
                    entry.total_commits_b,
                    entry.confidence,
                    entry.last_commit_sha,
                    entry.last_commit_ts,
                    mined_at,
                ])?;
                count += 1;
            }
        }

        tx.commit()?;
        Ok(count)
    }

    pub fn co_changes_for_file(
        &self,
        path: &str,
        limit: usize,
        min_confidence: f64,
        _graph_version: i64,
    ) -> Result<Vec<crate::model::CoChangeResult>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT file_a, file_b, co_change_count, confidence, last_commit_sha
             FROM co_changes
             WHERE (file_a = ? OR file_b = ?)
               AND confidence >= ?
             ORDER BY confidence DESC
             LIMIT ?",
        )?;

        let rows = stmt.query_map(params![path, path, min_confidence, limit as i64], |row| {
            Ok(crate::model::CoChangeResult {
                file_a: row.get(0)?,
                file_b: row.get(1)?,
                co_change_count: row.get(2)?,
                confidence: row.get(3)?,
                last_commit_sha: row.get(4)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn co_changes_for_files(
        &self,
        paths: &[String],
        limit: usize,
        min_confidence: f64,
        _graph_version: i64,
    ) -> Result<Vec<crate::model::CoChangeResult>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.read_conn()?;

        // Build query with IN clause
        let placeholders = paths.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT file_a, file_b, co_change_count, confidence, last_commit_sha
             FROM co_changes
             WHERE (file_a IN ({}) OR file_b IN ({}))
               AND confidence >= ?
             ORDER BY confidence DESC
             LIMIT ?",
            placeholders, placeholders
        );

        let mut stmt = conn.prepare(&sql)?;

        // Build params: paths, paths, min_confidence, limit
        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
        for path in paths {
            params.push(path);
        }
        for path in paths {
            params.push(path);
        }
        params.push(&min_confidence);
        let limit_param = limit as i64;
        params.push(&limit_param);

        let rows = stmt.query_map(&*params, |row| {
            Ok(crate::model::CoChangeResult {
                file_a: row.get(0)?,
                file_b: row.get(1)?,
                co_change_count: row.get(2)?,
                confidence: row.get(3)?,
                last_commit_sha: row.get(4)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn clear_co_changes(&mut self) -> Result<()> {
        self.conn().execute("DELETE FROM co_changes", [])?;
        Ok(())
    }

    pub fn coupling_hotspots(
        &self,
        limit: usize,
        min_confidence: f64,
    ) -> Result<Vec<crate::model::CouplingHotspot>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT file_a, file_b, confidence, co_change_count
             FROM co_changes
             WHERE confidence >= ?
             ORDER BY confidence DESC
             LIMIT ?",
        )?;

        let rows = stmt.query_map(params![min_confidence, limit as i64], |row| {
            Ok(crate::model::CouplingHotspot {
                file_a: row.get(0)?,
                file_b: row.get(1)?,
                confidence: row.get(2)?,
                co_change_count: row.get(3)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn digest(&self) -> Result<DbDigest> {
        Ok(DbDigest {
            files: self.digest_files()?,
            symbols: self.digest_symbols()?,
            edges: self.digest_edges()?,
        })
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

    fn digest_files(&self) -> Result<TableDigest> {
        let graph_version = self.current_graph_version()?;
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT path, hash, language, size, deleted_version
             FROM files
             WHERE deleted_version IS NULL OR deleted_version > ?
             ORDER BY path",
        )?;
        let rows = stmt.query_map(params![graph_version], |row| {
            let path: String = row.get(0)?;
            let hash: String = row.get(1)?;
            let language: String = row.get(2)?;
            let size: i64 = row.get(3)?;
            let deleted_version: Option<i64> = row.get(4)?;
            Ok(json!([path, hash, language, size, deleted_version]).to_string())
        })?;
        digest_rows(rows)
    }

    fn digest_symbols(&self) -> Result<TableDigest> {
        let graph_version = self.current_graph_version()?;
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
             ORDER BY f.path, s.qualname, s.kind, s.start_line, s.start_col, s.end_line, s.end_col",
        )?;
        let rows = stmt.query_map(params![graph_version, graph_version], |row| {
            let path: String = row.get(0)?;
            let kind: String = row.get(1)?;
            let name: String = row.get(2)?;
            let qualname: String = row.get(3)?;
            let start_line: i64 = row.get(4)?;
            let start_col: i64 = row.get(5)?;
            let end_line: i64 = row.get(6)?;
            let end_col: i64 = row.get(7)?;
            let start_byte: i64 = row.get(8)?;
            let end_byte: i64 = row.get(9)?;
            let signature: Option<String> = row.get(10)?;
            let docstring: Option<String> = row.get(11)?;
            let row_graph_version: i64 = row.get(12)?;
            let commit_sha: Option<String> = row.get(13)?;
            let stable_id: Option<String> = row.get(14)?;
            Ok(json!([
                path,
                kind,
                name,
                qualname,
                start_line,
                start_col,
                end_line,
                end_col,
                start_byte,
                end_byte,
                signature,
                docstring,
                row_graph_version,
                commit_sha,
                stable_id
            ])
            .to_string())
        })?;
        digest_rows(rows)
    }

    fn digest_edges(&self) -> Result<TableDigest> {
        let graph_version = self.current_graph_version()?;
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT f.path,
                    e.kind,
                    src.qualname,
                    COALESCE(tgt.qualname, e.target_qualname),
                    e.detail,
                    e.evidence_snippet,
                    e.evidence_start_line,
                    e.evidence_end_line,
                    e.confidence,
                    e.graph_version,
                    e.commit_sha,
                    e.trace_id,
                    e.span_id,
                    e.event_ts
             FROM edges e
             JOIN files f ON e.file_id = f.id
             LEFT JOIN symbols src ON e.source_symbol_id = src.id
             LEFT JOIN symbols tgt ON e.target_symbol_id = tgt.id
             WHERE e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
             ORDER BY f.path,
                      e.kind,
                      COALESCE(src.qualname, ''),
                      COALESCE(tgt.qualname, e.target_qualname, ''),
                      COALESCE(e.detail, ''),
                      COALESCE(e.evidence_snippet, '')",
        )?;
        let rows = stmt.query_map(params![graph_version, graph_version], |row| {
            let path: String = row.get(0)?;
            let kind: String = row.get(1)?;
            let source: Option<String> = row.get(2)?;
            let target: Option<String> = row.get(3)?;
            let detail: Option<String> = row.get(4)?;
            let evidence_snippet: Option<String> = row.get(5)?;
            let evidence_start_line: Option<i64> = row.get(6)?;
            let evidence_end_line: Option<i64> = row.get(7)?;
            let confidence: Option<f64> = row.get(8)?;
            let row_graph_version: i64 = row.get(9)?;
            let commit_sha: Option<String> = row.get(10)?;
            let trace_id: Option<String> = row.get(11)?;
            let span_id: Option<String> = row.get(12)?;
            let event_ts: Option<i64> = row.get(13)?;
            Ok(json!([
                path,
                kind,
                source,
                target,
                detail,
                evidence_snippet,
                evidence_start_line,
                evidence_end_line,
                confidence,
                row_graph_version,
                commit_sha,
                trace_id,
                span_id,
                event_ts
            ])
            .to_string())
        })?;
        digest_rows(rows)
    }

    /// Get module-level aggregation: file counts and symbol counts per directory prefix
    pub fn module_summary(
        &self,
        depth: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<ModuleSummaryEntry>> {
        let conn = self.read_conn()?;

        // Query all files with their symbol counts
        let mut sql = String::from(
            "SELECT f.path, f.language, COUNT(s.id) as sym_count
             FROM files f
             LEFT JOIN symbols s ON s.file_id = f.id AND s.graph_version = ?
             WHERE (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];

        // Add language filter
        if let Some(languages) = languages {
            if !languages.is_empty() {
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
        }

        // Add path filter
        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");

        sql.push_str(" GROUP BY f.id");

        let mut stmt = conn.prepare(&sql)?;
        let rows: Vec<(String, String, i64)> = stmt
            .query_map(&*params, |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })?
            .filter_map(|r| r.ok())
            .collect();

        // Group by module prefix at given depth
        let mut modules: std::collections::HashMap<
            String,
            (usize, usize, std::collections::HashSet<String>),
        > = std::collections::HashMap::new();

        for (path, lang, sym_count) in &rows {
            // Extract module prefix at depth
            let parts: Vec<&str> = path.split('/').collect();
            let prefix = if parts.len() > depth {
                parts[..depth].join("/") + "/"
            } else {
                // For files at shallower depth, use parent directory
                if parts.len() > 1 {
                    parts[..parts.len().saturating_sub(1)].join("/") + "/"
                } else {
                    ".".to_string()
                }
            };

            let entry = modules
                .entry(prefix)
                .or_insert((0, 0, std::collections::HashSet::new()));
            entry.0 += 1; // file count
            entry.1 += *sym_count as usize; // symbol count
            entry.2.insert(lang.clone()); // languages
        }

        let mut result: Vec<ModuleSummaryEntry> = modules
            .into_iter()
            .map(|(path, (fc, sc, langs))| ModuleSummaryEntry {
                path,
                file_count: fc,
                symbol_count: sc,
                languages: langs.into_iter().collect(),
            })
            .collect();
        result.sort_by(|a, b| b.symbol_count.cmp(&a.symbol_count));

        Ok(result)
    }

    /// Get inter-module edge counts (calls and imports)
    pub fn module_edges(
        &self,
        depth: usize,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<(String, String, usize, usize)>> {
        let conn = self.read_conn()?;

        // Query all CALLS, IMPORTS, and XREF edges with source and target file paths
        let mut sql = String::from(
            "SELECT e.kind, src_f.path as src_path, tgt_f.path as tgt_path
             FROM edges e
             JOIN symbols src_s ON e.source_symbol_id = src_s.id
             JOIN files src_f ON src_s.file_id = src_f.id
             LEFT JOIN symbols tgt_s ON e.target_symbol_id = tgt_s.id
             LEFT JOIN files tgt_f ON tgt_s.file_id = tgt_f.id
             WHERE e.kind IN ('CALLS', 'IMPORTS', 'XREF')
               AND e.graph_version = ?
               AND (src_f.deleted_version IS NULL OR src_f.deleted_version > ?)
               AND (tgt_f.deleted_version IS NULL OR tgt_f.deleted_version > ? OR tgt_f.id IS NULL)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> =
            vec![&graph_version, &graph_version, &graph_version];

        // Add language filter
        if let Some(languages) = languages {
            if !languages.is_empty() {
                sql.push_str(" AND src_f.language IN (");
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
        }

        let mut stmt = conn.prepare(&sql)?;
        let rows: Vec<(String, String, Option<String>)> = stmt
            .query_map(&*params, |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .filter_map(|r| r.ok())
            .collect();

        // Group by source module -> target module
        let mut edge_map: std::collections::HashMap<(String, String), (usize, usize)> =
            std::collections::HashMap::new();

        for (kind, src_path, tgt_path_opt) in &rows {
            // Extract module prefix at depth for source
            let src_parts: Vec<&str> = src_path.split('/').collect();
            let src_module = if src_parts.len() > depth {
                src_parts[..depth].join("/") + "/"
            } else if src_parts.len() > 1 {
                src_parts[..src_parts.len().saturating_sub(1)].join("/") + "/"
            } else {
                ".".to_string()
            };

            // Extract module prefix for target (if exists)
            if let Some(tgt_path) = tgt_path_opt {
                let tgt_parts: Vec<&str> = tgt_path.split('/').collect();
                let tgt_module = if tgt_parts.len() > depth {
                    tgt_parts[..depth].join("/") + "/"
                } else if tgt_parts.len() > 1 {
                    tgt_parts[..tgt_parts.len().saturating_sub(1)].join("/") + "/"
                } else {
                    ".".to_string()
                };

                // Skip self-edges (within same module)
                if src_module == tgt_module {
                    continue;
                }

                let key = (src_module.clone(), tgt_module.clone());
                let entry = edge_map.entry(key).or_insert((0, 0));

                if kind == "CALLS" || kind == "XREF" {
                    entry.0 += 1; // call count (includes XREFs as they're usage relationships)
                } else if kind == "IMPORTS" {
                    entry.1 += 1; // import count
                }
            }
        }

        let mut result: Vec<_> = edge_map
            .into_iter()
            .map(|((src, tgt), (calls, imports))| (src, tgt, calls, imports))
            .collect();
        result.sort_by(|a, b| (b.2 + b.3).cmp(&(a.2 + a.3))); // Sort by total edge count desc

        Ok(result)
    }
}

fn digest_rows<I>(rows: I) -> Result<TableDigest>
where
    I: Iterator<Item = rusqlite::Result<String>>,
{
    let mut hasher = Hasher::new();
    let mut count = 0;
    for row in rows {
        let row = row?;
        hasher.update(row.as_bytes());
        hasher.update(b"\n");
        count += 1;
    }
    Ok(TableDigest {
        rows: count,
        hash: hasher.finalize().to_hex().to_string(),
    })
}

fn count_files_for_version(
    conn: &Connection,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<i64> {
    let mut sql = String::from(
        "SELECT COUNT(*)
         FROM files f
         WHERE (f.deleted_version IS NULL OR f.deleted_version > ?)",
    );
    let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version];
    if let Some(languages) = languages {
        if !languages.is_empty() {
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
    }
    let count: i64 = conn.query_row(&sql, &*params, |row| row.get(0))?;
    Ok(count)
}

fn count_symbols_for_version(
    conn: &Connection,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<i64> {
    let mut sql = String::from(
        "SELECT COUNT(*)
         FROM symbols s
         JOIN files f ON s.file_id = f.id
         WHERE s.graph_version = ?
           AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
    );
    let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];
    if let Some(languages) = languages {
        if !languages.is_empty() {
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
    }
    let count: i64 = conn.query_row(&sql, &*params, |row| row.get(0))?;
    Ok(count)
}

fn count_edges_for_version(
    conn: &Connection,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<i64> {
    let mut sql = String::from(
        "SELECT COUNT(*)
         FROM edges e
         JOIN files f ON e.file_id = f.id
         WHERE e.graph_version = ?
           AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
    );
    let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];
    if let Some(languages) = languages {
        if !languages.is_empty() {
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
    }
    let count: i64 = conn.query_row(&sql, &*params, |row| row.get(0))?;
    Ok(count)
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
    for idx in base..path_params.len() {
        params.push(&path_params[idx] as &dyn rusqlite::ToSql);
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

fn diagnostic_from_row(row: &Row<'_>) -> rusqlite::Result<Diagnostic> {
    Ok(Diagnostic {
        id: row.get(0)?,
        path: row.get(1)?,
        line: row.get(2)?,
        column: row.get(3)?,
        end_line: row.get(4)?,
        end_column: row.get(5)?,
        severity: row.get(6)?,
        message: row.get(7)?,
        rule_id: row.get(8)?,
        tool: row.get(9)?,
        snippet: row.get(10)?,
    })
}

fn append_diagnostics_filters<'a>(
    sql: &mut String,
    params: &mut Vec<&'a dyn rusqlite::ToSql>,
    path_params: &'a mut Vec<String>,
    languages: Option<&'a [String]>,
    paths: Option<&'a [String]>,
    severity: Option<&'a String>,
    rule_id: Option<&'a String>,
    tool: Option<&'a String>,
) {
    fn push_clause(sql: &mut String, has_where: &mut bool, clause: &str) {
        if !*has_where {
            sql.push_str(" WHERE ");
            *has_where = true;
        } else {
            sql.push_str(" AND ");
        }
        sql.push_str(clause);
    }

    let mut has_where = false;
    let prefixes = collect_path_prefixes(paths);
    if !prefixes.is_empty() {
        push_clause(sql, &mut has_where, "(");
        path_params.reserve(prefixes.len().saturating_mul(2));
        let base = path_params.len();
        for (idx, prefix) in prefixes.iter().enumerate() {
            if idx > 0 {
                sql.push_str(" OR ");
            }
            sql.push_str("d.path = ? OR d.path LIKE ? ESCAPE '\\'");
            path_params.push(prefix.clone());
            let escaped = escape_like(prefix);
            path_params.push(format!("{escaped}/%"));
        }
        sql.push(')');
        for idx in base..path_params.len() {
            params.push(&path_params[idx] as &dyn rusqlite::ToSql);
        }
    }
    if let Some(severity) = severity {
        push_clause(sql, &mut has_where, "d.severity = ?");
        params.push(severity as &dyn rusqlite::ToSql);
    }
    if let Some(rule_id) = rule_id {
        push_clause(sql, &mut has_where, "d.rule_id = ?");
        params.push(rule_id as &dyn rusqlite::ToSql);
    }
    if let Some(tool) = tool {
        push_clause(sql, &mut has_where, "d.tool = ?");
        params.push(tool as &dyn rusqlite::ToSql);
    }
    if let Some(languages) = languages {
        if !languages.is_empty() {
            push_clause(sql, &mut has_where, "f.language IN (");
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
    }
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
}
