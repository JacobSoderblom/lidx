use crate::config::Config;
use crate::indexer::channel::is_bridge_edge_kind;
use crate::indexer::differ::SymbolDiff;
#[cfg(test)]
use crate::indexer::extract::ReceiverType;
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

/// Number of most-recent graph versions whose `symbols`/`edges` rows survive
/// `prune_old_graph_versions`. `carry_forward_files` (reindex's unchanged-file
/// fast path) is the only code that ever reads a `graph_version` other than
/// "current" for symbol/edge rows, and it only ever reads one version back
/// (`previous_graph_version`); the historical-impact, co-change and git-mining
/// features (src/impact/layers/historical.rs, src/db/co_change.rs,
/// src/git_mining.rs) read the version-independent `co_changes` table or git
/// itself, never old `symbols`/`edges` rows. 3 keeps that one required version
/// plus a spare for an in-flight reader that captured "current" just before a
/// reindex advanced it.
pub const DEFAULT_GRAPH_VERSION_RETENTION: i64 = 3;

/// Only run `VACUUM` when pruning actually freed at least this many bytes.
/// `VACUUM` rewrites the whole file, which is expensive on a large database;
/// a reindex that pruned nothing (or one old, mostly-carried-forward version)
/// shouldn't pay that cost every time.
const VACUUM_RECLAIM_THRESHOLD_BYTES: i64 = 10 * 1024 * 1024;

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
        // NULL out edges in other files that reference this file's symbols BEFORE
        // deleting them. SQLite reuses freed rowids (INTEGER PRIMARY KEY without
        // AUTOINCREMENT), so a reference that survives the deletion could silently
        // re-point at an unrelated symbol indexed later in the same sync.
        for column in ["source_symbol_id", "target_symbol_id"] {
            self.conn().execute(
                &format!(
                    "UPDATE edges SET {column} = NULL
                     WHERE {column} IN (
                         SELECT id FROM symbols WHERE file_id = ?1 AND graph_version = ?2
                     )
                     AND graph_version = ?2 AND file_id != ?1"
                ),
                params![file_id, graph_version],
            )?;
        }
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

    /// Carry forward symbols, edges, and symbol_metrics for files whose content
    /// hash is unchanged between `from_version` and `to_version`, instead of
    /// re-parsing them.
    ///
    /// `symbols.id` is `INTEGER PRIMARY KEY`, so a plain `INSERT ... SELECT` gives
    /// the copied rows fresh ids; `stable_id` is preserved on the copy, which is
    /// what lets the edge and symbol_metrics copies below re-target the new rows
    /// instead of the old (now stale) ones. Edge endpoints and symbol_metrics'
    /// `symbol_id` are remapped the same way: joining each old row's symbol to
    /// whichever `to_version` row shares its `stable_id`. `file_metrics` needs no
    /// such copy — it's keyed by `file_id` alone (no `graph_version` column), and
    /// `files.id` doesn't change across versions, so an unchanged file's existing
    /// row is already correctly attached.
    ///
    /// Callers must run this only after every `to_version` symbol write for this
    /// reindex has happened, including freshly re-parsed files — a carried edge
    /// whose target lives in a re-parsed file won't resolve until that file's new
    /// symbol row exists.
    ///
    /// Returns `(symbols_copied, edges_copied)`.
    pub fn carry_forward_files(
        &self,
        file_ids: &[i64],
        from_version: i64,
        to_version: i64,
    ) -> Result<(usize, usize)> {
        if file_ids.is_empty() {
            return Ok((0, 0));
        }

        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let placeholders = vec!["?"; file_ids.len()].join(",");

        let symbols_copied = {
            let sql = format!(
                "INSERT INTO symbols
                    (file_id, kind, name, qualname, start_line, start_col, end_line, end_col,
                     start_byte, end_byte, signature, docstring, graph_version, commit_sha, stable_id)
                 SELECT file_id, kind, name, qualname, start_line, start_col, end_line, end_col,
                        start_byte, end_byte, signature, docstring, ?, commit_sha, stable_id
                 FROM symbols
                 WHERE graph_version = ? AND file_id IN ({placeholders})"
            );
            let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                vec![Box::new(to_version), Box::new(from_version)];
            for id in file_ids {
                params.push(Box::new(*id));
            }
            tx.execute(
                &sql,
                rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
            )?
        };

        // ponytail: an edge endpoint with no stable_id match in `to_version`
        // (deleted target, or a stable_id collision) is copied with that endpoint
        // NULL rather than dropped — the same best-effort contract the rest of the
        // edge-resolution code (insert_edges' fuzzy fallback, resolve_null_target_edges)
        // already has for unresolved targets.
        let edges_copied = {
            let sql = format!(
                "INSERT INTO edges
                    (file_id, source_symbol_id, target_symbol_id, kind, target_qualname, detail,
                     evidence_snippet, evidence_start_line, evidence_end_line, confidence,
                     graph_version, commit_sha, trace_id, span_id, event_ts,
                     receiver_type, resolution_kind, import_candidates)
                 SELECT
                    e.file_id,
                    (SELECT ns.id FROM symbols ns
                        WHERE ns.stable_id = src.stable_id AND ns.graph_version = ? LIMIT 1),
                    (SELECT nt.id FROM symbols nt
                        WHERE nt.stable_id = tgt.stable_id AND nt.graph_version = ? LIMIT 1),
                    e.kind, e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    ?, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.receiver_type, e.resolution_kind, e.import_candidates
                 FROM edges e
                 LEFT JOIN symbols src ON src.id = e.source_symbol_id
                 LEFT JOIN symbols tgt ON tgt.id = e.target_symbol_id
                 WHERE e.graph_version = ? AND e.file_id IN ({placeholders})"
            );
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![
                Box::new(to_version),
                Box::new(to_version),
                Box::new(to_version),
                Box::new(from_version),
            ];
            for id in file_ids {
                params.push(Box::new(*id));
            }
            tx.execute(
                &sql,
                rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
            )?
        };

        // Copy `symbol_metrics` for the symbols just copied above, remapped from
        // each old symbol id to its `to_version` counterpart by `stable_id` (the
        // same key the edge copy above uses). Without this, an unchanged file's
        // metrics stay attached to the old, soon-to-be-pruned version's symbol
        // ids and metrics-backed queries (top_complexity, dead_symbols, ...) see
        // none for the current version.
        //
        // ponytail: unlike edges' nullable endpoints, `symbol_metrics.symbol_id`
        // is `NOT NULL UNIQUE`, so a row whose old symbol has no `stable_id`
        // match in `to_version` (NULL `stable_id`, or a collision the edge copy's
        // `LIMIT 1` didn't happen to pick) is dropped rather than inserted with a
        // NULL/dangling id. Ceiling: that symbol's metrics are lost for this
        // version instead of merely stale; the same rare conditions already make
        // its edges best-effort-NULL above.
        {
            let sql = format!(
                "INSERT INTO symbol_metrics (symbol_id, file_id, loc, complexity, duplication_hash)
                 SELECT
                    (SELECT ns.id FROM symbols ns
                        WHERE ns.stable_id = os.stable_id AND ns.graph_version = ? LIMIT 1),
                    sm.file_id, sm.loc, sm.complexity, sm.duplication_hash
                 FROM symbol_metrics sm
                 JOIN symbols os ON os.id = sm.symbol_id
                 WHERE os.graph_version = ? AND os.file_id IN ({placeholders})
                   AND (SELECT ns.id FROM symbols ns
                        WHERE ns.stable_id = os.stable_id AND ns.graph_version = ? LIMIT 1) IS NOT NULL"
            );
            let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                vec![Box::new(to_version), Box::new(from_version)];
            for id in file_ids {
                params.push(Box::new(*id));
            }
            // The trailing `IS NOT NULL` guard's `?` binds after the `IN (...)`
            // placeholders above it in the SQL text.
            params.push(Box::new(to_version));
            tx.execute(
                &sql,
                rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
            )?;
        }

        tx.commit()?;
        Ok((symbols_copied, edges_copied))
    }

    /// Delete `symbols`/`edges` rows for every graph version older than the
    /// `keep` most recent ones (see `DEFAULT_GRAPH_VERSION_RETENTION` for why
    /// `keep` is safe to set below the total version count). `symbol_metrics`
    /// rows for pruned symbols are removed via `ON DELETE CASCADE` (foreign
    /// keys are enabled on every connection, see `Db::new`).
    ///
    /// `graph_versions` (the id/created/commit_sha metadata rows), `files`,
    /// and `co_changes` are untouched: none of them are duplicated per
    /// reindex the way `symbols`/`edges` are, so none contribute to the
    /// unbounded growth this prunes.
    ///
    /// The retention boundary is found by position in `graph_versions`
    /// (Nth most recent id), not by arithmetic on the current version number,
    /// so it stays correct even if version ids are ever non-contiguous.
    ///
    /// Returns `(symbols_deleted, edges_deleted, versions_pruned)`.
    pub fn prune_old_graph_versions(&self, keep: i64) -> Result<(usize, usize, usize)> {
        let keep = keep.max(1);
        let mut conn = self.conn();
        let tx = conn.transaction()?;

        let boundary: Option<i64> = tx
            .query_row(
                "SELECT id FROM graph_versions ORDER BY id DESC LIMIT 1 OFFSET ?",
                params![keep - 1],
                |row| row.get(0),
            )
            .optional()?;
        let Some(boundary) = boundary else {
            // Fewer than `keep` versions exist yet; nothing to prune.
            return Ok((0, 0, 0));
        };

        let versions_pruned: i64 = tx.query_row(
            "SELECT COUNT(*) FROM graph_versions WHERE id < ?",
            params![boundary],
            |row| row.get(0),
        )?;
        let edges_deleted = tx.execute(
            "DELETE FROM edges WHERE graph_version < ?",
            params![boundary],
        )?;
        let symbols_deleted = tx.execute(
            "DELETE FROM symbols WHERE graph_version < ?",
            params![boundary],
        )?;

        tx.commit()?;
        Ok((symbols_deleted, edges_deleted, versions_pruned as usize))
    }

    /// Bytes SQLite could reclaim from the database file via `VACUUM` right now.
    pub fn freelist_bytes(&self) -> Result<i64> {
        let conn = self.conn();
        let freelist: i64 = conn.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
        let page_size: i64 = conn.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        Ok(freelist * page_size)
    }

    /// Rebuild the database file to reclaim space freed by deletes. Works in
    /// WAL mode (supported since SQLite 3.15): as part of `VACUUM`'s commit,
    /// SQLite truncates the WAL file too, so no separate checkpoint is needed.
    pub fn vacuum(&self) -> Result<()> {
        self.conn().execute_batch("VACUUM;")?;
        Ok(())
    }

    /// Prune graph versions beyond `DEFAULT_GRAPH_VERSION_RETENTION` and, if
    /// that freed a meaningful amount of space, reclaim it with `VACUUM`.
    /// Intended to run automatically at the end of every `reindex()`.
    ///
    /// // ponytail: only reachable by running a reindex (see `Indexer::reindex`)
    /// // — there's no standalone "just prune" CLI/RPC command. Ceiling: a
    /// // database that's too large/stale for `reindex` to complete (e.g. the
    /// // scan or carry-forward step itself times out or errors first) has no
    /// // way to reclaim space without fixing that first. Upgrade path: add a
    /// // thin `lidx prune --db <path>` subcommand (and/or RPC method) that
    /// // calls this directly, once that scenario actually comes up.
    ///
    /// Returns `(symbols_deleted, edges_deleted, versions_pruned, vacuumed)`.
    pub fn prune_and_maybe_vacuum(&self) -> Result<(usize, usize, usize, bool)> {
        let (symbols_deleted, edges_deleted, versions_pruned) =
            self.prune_old_graph_versions(DEFAULT_GRAPH_VERSION_RETENTION)?;

        let mut vacuumed = false;
        if versions_pruned > 0 {
            let reclaimable = self.freelist_bytes().unwrap_or(0);
            if reclaimable >= VACUUM_RECLAIM_THRESHOLD_BYTES {
                self.vacuum()?;
                vacuumed = true;
            }
        }

        Ok((symbols_deleted, edges_deleted, versions_pruned, vacuumed))
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
        // Before deleting, NULL out any edges in other files that reference these
        // symbols by rowid. If we delete first, SQLite may immediately reuse the
        // freed rowid for a new symbol (INTEGER PRIMARY KEY without AUTOINCREMENT),
        // which would make the edges appear valid after the fact.
        if !diff.deleted.is_empty() {
            let placeholders = vec!["?"; diff.deleted.len()].join(",");

            // Step 1a: Collect rowids of the symbols about to be deleted
            let rowid_sql = format!(
                "SELECT id FROM symbols WHERE stable_id IN ({}) AND graph_version = ?",
                placeholders
            );
            let deleted_rowids: Vec<i64> = {
                let mut stmt = tx.prepare(&rowid_sql)?;
                let rows = stmt.query_map(
                    rusqlite::params_from_iter(
                        diff.deleted
                            .iter()
                            .map(|stable_id| stable_id as &dyn rusqlite::ToSql)
                            .chain([&graph_version as &dyn rusqlite::ToSql]),
                    ),
                    |row| row.get::<_, i64>(0),
                )?;
                rows.collect::<Result<Vec<_>, _>>()?
            };

            // Step 1b: NULL out edges in other files that reference these rowids,
            // so no dangling reference survives the symbol deletion.
            if !deleted_rowids.is_empty() {
                let edge_placeholders = vec!["?"; deleted_rowids.len()].join(",");
                for column in ["source_symbol_id", "target_symbol_id"] {
                    tx.execute(
                        &format!(
                            "UPDATE edges SET {column} = NULL
                             WHERE {column} IN ({edge_placeholders})
                             AND graph_version = ? AND file_id != ?"
                        ),
                        rusqlite::params_from_iter(
                            deleted_rowids
                                .iter()
                                .map(|id| id as &dyn rusqlite::ToSql)
                                .chain([
                                    &graph_version as &dyn rusqlite::ToSql,
                                    &file_id as &dyn rusqlite::ToSql,
                                ]),
                        ),
                    )?;
                }
            }

            // Step 1c: Delete the symbols
            let delete_sql = format!(
                "DELETE FROM symbols WHERE stable_id IN ({}) AND graph_version = ?",
                placeholders
            );
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
                  evidence_start_line, evidence_end_line, confidence, graph_version, commit_sha, trace_id, span_id, event_ts,
                  receiver_type, resolution_kind, import_candidates)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            let mut exact_lookup_stmt = tx.prepare(
                "SELECT id FROM symbols WHERE qualname = ? AND graph_version = ? ORDER BY id ASC LIMIT 1",
            )?;
            // Same-language fuzzy lookup: prefer symbols from files matching source language.
            // LIMIT 2 (not 1): the ambiguity guard in `single_unambiguous_match` needs to see
            // a second candidate row to know the bare-name/suffix match is ambiguous.
            let mut fuzzy_same_lang_stmt = tx.prepare(
                "SELECT s.id
                 FROM symbols s
                 JOIN files f ON s.file_id = f.id
                 WHERE (s.qualname = ? OR s.qualname LIKE ? OR s.qualname LIKE ?)
                   AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
                   AND s.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                   AND (CASE WHEN f.language IN ('typescript', 'tsx') THEN 'javascript' ELSE f.language END) = ?
                 LIMIT 2"
            )?;
            // Cross-language fuzzy lookup: fallback for bridge edges only
            let mut fuzzy_any_lang_stmt = tx.prepare(
                "SELECT s.id
                 FROM symbols s
                 JOIN files f ON s.file_id = f.id
                 WHERE (s.qualname = ? OR s.qualname LIKE ? OR s.qualname LIKE ?)
                   AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
                   AND s.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                 LIMIT 2"
            )?;
            // Ancestor lookup for the inherited-method tier (see
            // `resolve_via_inheritance`): given a type's own symbol id, its
            // recorded EXTENDS/IMPLEMENTS/INHERITS edges in declaration
            // order. `edges.id` is insertion order, which mirrors source
            // order — each extractor emits a class's base-list edges in one
            // pass, in the order the bases are written.
            let mut hierarchy_stmt = tx.prepare(
                "SELECT target_symbol_id, target_qualname
                 FROM edges
                 WHERE source_symbol_id = ?
                   AND kind IN ('EXTENDS', 'IMPLEMENTS', 'INHERITS')
                   AND graph_version = ?
                   AND target_qualname IS NOT NULL
                 ORDER BY id ASC",
            )?;
            let mut import_suffix_stmt = tx.prepare(IMPORT_SUFFIX_LOOKUP_SQL)?;
            let mut repo_module_stmt = tx.prepare(
                "SELECT 1 FROM symbols s JOIN files f ON s.file_id = f.id
                 WHERE s.name = ? AND s.kind = 'module' AND f.language = 'python'
                 LIMIT 1",
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
                let source_id = resolve_symbol_id(
                    &edge.source_qualname,
                    symbol_map,
                    &mut exact_lookup_stmt,
                    graph_version,
                )?;
                let exact_target = resolve_symbol_id(
                    &edge.target_qualname,
                    symbol_map,
                    &mut exact_lookup_stmt,
                    graph_version,
                )?;
                // Exact qualname match is always tried first, regardless of
                // receiver_type — it's authoritative when it hits. On a
                // miss, try the import-qualified candidates (if any) next —
                // also an exact-match tier, just over several guesses
                // instead of one; still authoritative only when exactly one
                // resolves. Only after both miss do we consult the
                // receiver-type-gated fuzzy tiers (see `resolve_fuzzy_target`).
                //
                // `receiver_type_for_storage` starts as the extractor's own
                // signal and is only ever narrowed (never widened) below.
                let mut receiver_type_for_storage = edge.receiver_type.as_column();
                let (target_id, resolution_kind) = if exact_target.is_some() {
                    (exact_target, Some("exact"))
                } else if let Some(import_id) = resolve_import_candidate(
                    &edge.import_candidates,
                    symbol_map,
                    &mut exact_lookup_stmt,
                    &mut import_suffix_stmt,
                    graph_version,
                )? {
                    (Some(import_id), Some("import"))
                } else {
                    // `import_candidates` is populated only when the
                    // extractor already established (from this file's own
                    // using/import directives) that the receiver is bound
                    // by an import — see `EdgeInput::import_candidates`.
                    // `resolve_import_candidate` above just tried every one
                    // of those candidates and found no single unambiguous
                    // local symbol (0 hits, or 2+ distinct ones). That is
                    // positive information, not silence: the receiver is
                    // known to come from an import this repo's index
                    // doesn't (or can't uniquely) resolve — stdlib, a
                    // third-party package, a BCL type — or the import
                    // itself is ambiguous. Falling through to the blind
                    // two-segment/bare-name tiers would then bind on
                    // name-uniqueness alone, which is exactly the
                    // false-positive class this guards against (e.g.
                    // `datetime.now()` binding to an unrelated, uniquely-
                    // named local `FakeClock.now`). So treat it the same as
                    // a receiver-type-tracked-but-unresolved edge
                    // (`Some("")` — "must not bind, no lookup attempted at
                    // all"): both two-segment and bare-name are skipped,
                    // not just bare-name, since two-segment's literal
                    // suffix match is just as able to hit an unrelated
                    // same-named local symbol.
                    //
                    // Python only: an import rooted in a repo package (or a
                    // relative one) that still failed to resolve is most
                    // likely a re-export (`from pkg import X` where `pkg/
                    // __init__.py` re-exports X from a submodule). That is
                    // not evidence the target lives outside the repo, so
                    // it keeps the pre-existing fuzzy tiers rather than
                    // being refused. See `is_repo_python_import`. JS/TS
                    // gets no such exception: its extractor resolves the
                    // specifier to a file itself, so a miss there is a
                    // re-export or external package, and fuzzy binding
                    // would now reach across the whole ts/tsx/js family
                    // (see `resolution_language_family`).
                    if !edge.import_candidates.is_empty()
                        && (source_lang != "python"
                            || !is_repo_python_import(
                                &edge.import_candidates,
                                &mut repo_module_stmt,
                            )?)
                    {
                        receiver_type_for_storage = Some("");
                    }
                    match edge.target_qualname.as_deref() {
                        Some(qn) => resolve_fuzzy_target(
                            qn,
                            receiver_type_for_storage,
                            &edge.kind,
                            &source_lang,
                            graph_version,
                            &mut fuzzy_same_lang_stmt,
                            &mut fuzzy_any_lang_stmt,
                            &mut hierarchy_stmt,
                        )?,
                        None => (None, None),
                    }
                };

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
                    receiver_type_for_storage,
                    resolution_kind,
                    encode_import_candidates(&edge.import_candidates),
                ])?;
                count += 1;
            }
        }
        tx.commit()?;
        Ok(count)
    }

    /// Batch re-resolution of existing edges with NULL target_symbol_id
    ///
    /// This method attempts to resolve unresolved edges in three passes:
    /// 1. Exact match on target_qualname
    /// 2. Retry of the import-qualified-candidate tier (`resolve_import_candidate`)
    /// 3. Fuzzy suffix matching for remaining NULLs
    ///
    /// Processing is done in batches of 1000 rows to avoid long lock holds.
    ///
    /// ponytail: pass 2 only retries edges whose `import_candidates` column
    /// is non-NULL, i.e. ones inserted after migration 14 added that
    /// column. An edge from a build predating this feature (or one whose
    /// extractor never populates `import_candidates`, e.g. Rust/Go) has no import context to try and falls straight through to pass
    /// 3, unchanged from before. That's the pre-existing ceiling on this
    /// repair pass generally (see `repair_dangling_symbol_ids`'s doc), not
    /// a new one introduced here.
    pub fn resolve_null_target_edges(&self, graph_version: i64) -> Result<usize> {
        let mut total_resolved = 0;

        // First pass: exact match. Unconditional — exact qualname equality
        // is authoritative regardless of receiver_type. Tag resolution_kind
        // only for rows this pass actually binds (the correlated subquery
        // is evaluated against the pre-update row on both sides, so this is
        // unambiguous regardless of SQLite's SET-clause evaluation order).
        let exact_resolved = self.conn().execute(
            "UPDATE edges SET
                target_symbol_id = (
                    SELECT s.id FROM symbols s
                    WHERE s.qualname = edges.target_qualname
                    AND s.graph_version = edges.graph_version
                    ORDER BY s.id ASC
                    LIMIT 1
                ),
                resolution_kind = CASE WHEN (
                    SELECT s.id FROM symbols s
                    WHERE s.qualname = edges.target_qualname
                    AND s.graph_version = edges.graph_version
                    LIMIT 1
                ) IS NOT NULL THEN 'exact' ELSE resolution_kind END
            WHERE target_symbol_id IS NULL
            AND target_qualname IS NOT NULL
            AND graph_version = ?",
            params![graph_version],
        )?;
        total_resolved += exact_resolved;

        const BATCH_SIZE: usize = 1000;

        // Second pass: retry the import-qualified-candidate tier for edges
        // whose target wasn't resolvable yet at `insert_edges` time. This
        // closes the incremental-reindex gap `EdgeInput::import_candidates`
        // describes: during an incremental reindex, fresh files' edges are
        // inserted *before* unchanged files are carried forward into the
        // new graph version (see the ordering comment above
        // `carry_forward_files`'s call site in `Indexer::reindex`), so an
        // import candidate whose target lives in a carried-forward file has
        // no current-version symbol row to match yet. `insert_edges`
        // persists that as `target_symbol_id = NULL, receiver_type = ''`
        // (see the guard in `insert_edges` and its `import_candidates`
        // check) — deliberately, so pass 3 below refuses to fuzzy-resolve
        // it — but until this pass existed nothing ever retried the import
        // tier itself once the target's row showed up, so the edge stayed
        // unresolved forever. Still an exact-match tier, not fuzzy: reuses
        // `resolve_import_candidate`'s own ambiguity guard (0 or 2+
        // distinct hits across a row's candidates leaves it unresolved),
        // with an empty symbol_map since this pass has no in-flight batch
        // to consult — every candidate is looked up straight against the
        // DB.
        let empty_symbol_map: HashMap<String, i64> = HashMap::new();
        loop {
            let mut conn = self.conn();
            let tx = conn.transaction()?;

            let batch: Vec<(i64, String)> = {
                let mut stmt = tx.prepare(
                    "SELECT id, import_candidates FROM edges
                     WHERE target_symbol_id IS NULL
                     AND import_candidates IS NOT NULL
                     AND graph_version = ?
                     LIMIT ?",
                )?;
                let rows = stmt.query_map(params![graph_version, BATCH_SIZE], |row| {
                    Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
                })?;
                rows.collect::<Result<Vec<_>, _>>()?
            };

            if batch.is_empty() {
                break;
            }

            let mut count = 0;
            {
                let mut exact_lookup_stmt = tx.prepare(
                    "SELECT id FROM symbols WHERE qualname = ? AND graph_version = ? ORDER BY id ASC LIMIT 1",
                )?;
                let mut update_stmt = tx.prepare(
                    "UPDATE edges SET target_symbol_id = ?, resolution_kind = 'import' WHERE id = ?",
                )?;
                let mut import_suffix_stmt = tx.prepare(IMPORT_SUFFIX_LOOKUP_SQL)?;

                for (edge_id, candidates_json) in &batch {
                    let candidates = decode_import_candidates(candidates_json);
                    if let Some(target_id) = resolve_import_candidate(
                        &candidates,
                        &empty_symbol_map,
                        &mut exact_lookup_stmt,
                        &mut import_suffix_stmt,
                        graph_version,
                    )? {
                        update_stmt.execute(params![target_id, edge_id])?;
                        count += 1;
                    }
                }
            }

            tx.commit()?;
            total_resolved += count;

            // Every row in `batch` is either now resolved or was tried and
            // found unresolvable (0 or 2+ distinct hits) — nothing here
            // will change on a re-select, so a zero-progress batch means
            // stop, exactly like pass 3 below. Without this, a batch full
            // of unresolvable-but-still-NULL rows would re-select forever.
            if count == 0 {
                break;
            }
        }

        // Third pass: fuzzy suffix matching in batches
        loop {
            let mut conn = self.conn();
            let tx = conn.transaction()?;

            // Find batch of unresolved edges (include source file language, edge kind, and
            // the persisted receiver_type signal). Edges tracked as receiver_type = '' (a
            // builtin/unresolved receiver) are excluded here — they must never be
            // fuzzy-resolved, on this pass or any later repair pass.
            let unresolved: Vec<(i64, String, String, String, Option<String>)> = {
                let mut stmt = tx.prepare(
                    "SELECT e.id, e.target_qualname, COALESCE(f.language, 'unknown'), e.kind, e.receiver_type
                     FROM edges e
                     JOIN files f ON e.file_id = f.id
                     WHERE e.target_symbol_id IS NULL
                     AND e.target_qualname IS NOT NULL
                     AND e.graph_version = ?
                     AND (e.receiver_type IS NULL OR e.receiver_type != '')
                     LIMIT ?",
                )?;
                let rows = stmt.query_map(params![graph_version, BATCH_SIZE], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                })?;
                rows.collect::<Result<Vec<_>, _>>()?
            };

            if unresolved.is_empty() {
                break;
            }

            let mut count = 0;
            {
                // Same-language fuzzy lookup. LIMIT 2 (not 1): the ambiguity guard in
                // `single_unambiguous_match` needs a second candidate row to detect that the
                // bare-name/suffix match is ambiguous.
                let mut fuzzy_same_lang_stmt = tx.prepare(
                    "SELECT s.id
                     FROM symbols s
                     JOIN files f ON s.file_id = f.id
                     WHERE (s.qualname = ? OR s.qualname LIKE ? OR s.qualname LIKE ?)
                       AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
                       AND s.graph_version = ?
                       AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                       AND (CASE WHEN f.language IN ('typescript', 'tsx') THEN 'javascript' ELSE f.language END) = ?
                     LIMIT 2"
                )?;
                // Cross-language fuzzy lookup (for bridge edges only)
                let mut fuzzy_any_lang_stmt = tx.prepare(
                    "SELECT s.id
                     FROM symbols s
                     JOIN files f ON s.file_id = f.id
                     WHERE (s.qualname = ? OR s.qualname LIKE ? OR s.qualname LIKE ?)
                       AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
                       AND s.graph_version = ?
                       AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                     LIMIT 2"
                )?;
                // Ancestor lookup for the inherited-method tier — see the
                // identical statement (and its comment) in `insert_edges`.
                let mut hierarchy_stmt = tx.prepare(
                    "SELECT target_symbol_id, target_qualname
                     FROM edges
                     WHERE source_symbol_id = ?
                       AND kind IN ('EXTENDS', 'IMPLEMENTS', 'INHERITS')
                       AND graph_version = ?
                       AND target_qualname IS NOT NULL
                     ORDER BY id ASC",
                )?;

                let mut update_stmt = tx.prepare(
                    "UPDATE edges SET target_symbol_id = ?, resolution_kind = ? WHERE id = ?",
                )?;

                for (edge_id, target_qualname, source_lang, edge_kind, receiver_type) in &unresolved
                {
                    let (resolved, resolution_kind) = resolve_fuzzy_target(
                        target_qualname,
                        receiver_type.as_deref(),
                        edge_kind,
                        source_lang,
                        graph_version,
                        &mut fuzzy_same_lang_stmt,
                        &mut fuzzy_any_lang_stmt,
                        &mut hierarchy_stmt,
                    )?;

                    if let Some(symbol_id) = resolved {
                        update_stmt.execute(params![symbol_id, resolution_kind, edge_id])?;
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

    /// Null out edge source/target symbol ids that reference rowids absent from the
    /// current graph version's symbols table.
    ///
    /// This is necessary after an incremental sync because:
    /// - Renaming a symbol deletes its old row and inserts a new one with a fresh rowid.
    /// - Edges in **unchanged** files still carry the old rowid in source_symbol_id /
    ///   target_symbol_id (no FK enforcement, so they silently dangle).
    /// - SQLite reuses freed rowids (INTEGER PRIMARY KEY without AUTOINCREMENT), so the
    ///   dangling id can silently point at a new unrelated symbol.
    ///
    /// Setting dangling ids to NULL lets `resolve_null_target_edges` re-resolve them
    /// by qualname in a subsequent pass.
    ///
    /// Returns the number of edges updated.
    pub fn repair_dangling_symbol_ids(&self, graph_version: i64) -> Result<usize> {
        let mut total = 0;
        for column in ["source_symbol_id", "target_symbol_id"] {
            total += self.conn().execute(
                &format!(
                    "UPDATE edges
                     SET {column} = NULL
                     WHERE {column} IS NOT NULL
                       AND graph_version = ?
                       AND {column} NOT IN (
                           SELECT id FROM symbols WHERE graph_version = ?
                       )"
                ),
                params![graph_version, graph_version],
            )?;
        }
        Ok(total)
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
    /// Used by symbol resolution, context assembly, and incremental reindexing
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

/// Extract the trailing name segment from a qualname, handling both `.` and `::` separators.
///
/// Examples:
/// - `"a.b.process"` → `"process"`
/// - `"crate::util::helper::process"` → `"process"`
/// - `"_svc.DeployAsync"` → `"DeployAsync"`
/// - `"process"` → `"process"` (no separator)
pub(crate) fn qualname_trailing_name(qn: &str) -> &str {
    // Find the last occurrence of either '.' or "::"
    let dot_pos = qn.rfind('.').map(|p| p + 1);
    let colons_pos = qn.rfind("::").map(|p| p + 2);
    match (dot_pos, colons_pos) {
        (Some(d), Some(c)) => &qn[d.max(c)..],
        (Some(d), None) => &qn[d..],
        (None, Some(c)) => &qn[c..],
        (None, None) => qn,
    }
}

/// Build the fuzzy suffix-match inputs for a target qualname: the trailing
/// name (for exact matching) plus LIKE patterns for both `.`- and
/// `::`-separated qualnames.
///
/// Shared by all fuzzy edge-resolution sites (`insert_edges`,
/// `resolve_null_target_edges`, `lookup_symbol_id_fuzzy`) so the matching
/// logic cannot drift between them. Deliberately no bare `%name` pattern:
/// that would let `process` match `reprocess`.
pub(crate) fn fuzzy_qualname_patterns(qn: &str) -> (&str, String, String) {
    let name = qualname_trailing_name(qn);
    (name, format!("%.{name}"), format!("%::{name}"))
}

/// Locate the last qualname separator (`.` or `::`) in `s`, returning the
/// index right after it (the start of the trailing segment). `None` if `s`
/// has no separator. Factored out of `qualname_trailing_name`'s inline logic
/// so `qualname_trailing_two_segments` can reuse it without changing that
/// function's shared behavior.
fn last_qualname_separator(s: &str) -> Option<usize> {
    let dot_pos = s.rfind('.').map(|p| p + 1);
    let colons_pos = s.rfind("::").map(|p| p + 2);
    match (dot_pos, colons_pos) {
        (Some(d), Some(c)) => Some(d.max(c)),
        (Some(d), None) => Some(d),
        (None, Some(c)) => Some(c),
        (None, None) => None,
    }
}

/// Extract the trailing **two** qualname segments (handling both `.` and
/// `::` separators), when the qualname has more than one segment.
///
/// Examples:
/// - `"crate::db::Db::new"` -> `Some("Db::new")`
/// - `"Vec::new"` -> `Some("Vec::new")`
/// - `"pkg.store.EventStore.append"` -> `Some("EventStore.append")`
/// - `"process"` -> `None` (only one segment, nothing to disambiguate with)
fn qualname_trailing_two_segments(qn: &str) -> Option<&str> {
    let name_start = last_qualname_separator(qn)?;
    let prefix = qn[..name_start].trim_end_matches(['.', ':']);
    if prefix.is_empty() {
        return None;
    }
    let seg2_start = last_qualname_separator(prefix).unwrap_or(0);
    Some(&qn[seg2_start..])
}

/// Build the fuzzy suffix-match inputs for a call site's last **two**
/// qualname segments (`Type::method` / `Type.method`), used only by the
/// two-segment resolution tier in `insert_edges` / `resolve_null_target_edges`.
/// Returns `None` when the qualname carries only one segment; callers fall
/// through to the existing bare-name tier (`fuzzy_qualname_patterns`) in that
/// case. Deliberately additive: `fuzzy_qualname_patterns` itself, and every
/// other caller of it (`lookup_symbol_id_fuzzy` and its dependents), are
/// untouched.
///
/// ponytail: matching on the last two segments *already present in the
/// stored target_qualname string* is the cheap fix for the common
/// `Type::method` call shape (`Db::new` finds `crate::db::Db::new` without
/// colliding with `Vec::new`). It is not receiver-type inference — no type
/// resolution, no import tracking — so it won't help qualnames that only
/// ever carry one segment, or disambiguate two same-named two-segment calls
/// on unrelated types that happen to share both segments. Resolving the
/// receiver's actual type before matching the method is the upgrade path if
/// this ceiling proves too low.
pub(crate) fn two_segment_qualname_patterns(qn: &str) -> Option<(String, String, String)> {
    let two = qualname_trailing_two_segments(qn)?;
    Some((two.to_string(), format!("%.{two}"), format!("%::{two}")))
}

/// Ambiguity guard for bare-name/suffix fuzzy edge resolution.
///
/// `stmt` must be a query shaped `... LIMIT 2`. A call site resolved by bare
/// method name (no receiver-type information) is only trustworthy when
/// exactly one same-named candidate survives the kind/language/version
/// filters; if a second row shows up, the name is ambiguous (e.g. `.append`
/// matching both `list.append` and a domain `EventStore.append`) and we
/// return `None` rather than binding to whichever row SQLite happened to
/// return first.
///
/// ponytail: candidate-count(<=1) is the cheapest signal that fixes the
/// observed over-binding without receiver-type inference; if this proves too
/// coarse, real disambiguation (resolving the receiver's type before matching
/// the method) is the upgrade path, not a bigger threshold.
fn single_unambiguous_match(
    stmt: &mut rusqlite::Statement<'_>,
    query_params: &[&dyn rusqlite::ToSql],
) -> rusqlite::Result<Option<i64>> {
    let mut rows = stmt.query(query_params)?;
    let id = match rows.next()? {
        Some(row) => row.get::<_, i64>(0)?,
        None => return Ok(None),
    };
    if rows.next()?.is_some() {
        return Ok(None);
    }
    Ok(Some(id))
}

/// The language value the same-language fuzzy tiers compare against.
/// `typescript`, `tsx` and `javascript` are separate `files.language` values
/// only because each needs its own tree-sitter grammar; a `.tsx` file calls
/// into `.ts` modules as a matter of course, so for resolution they are one
/// family. Must agree with the `CASE` on `f.language` in the same-language
/// fuzzy statements.
///
/// Relaxing the gate is safe from the `FakeClock.now` class of bug (commit
/// 1d6a5a7) only because the JS/TS extractor records an import candidate
/// for every call through an import binding: when that candidate misses
/// (external package, re-export), `insert_edges` refuses the fuzzy tiers
/// outright, so a bare call to an imported name never binds by uniqueness.
fn resolution_language_family(lang: &str) -> &str {
    match lang {
        "typescript" | "tsx" => "javascript",
        other => other,
    }
}

/// Resolve a CALLS-shaped edge's target through the fuzzy tiers, gated by
/// its `receiver_type` signal (see `edges.receiver_type` / `ReceiverType`).
/// Shared by `insert_edges` and `resolve_null_target_edges` so the two
/// resolution paths cannot drift apart.
///
/// `receiver_type`: `None` = not tracked by the extractor — run the
/// pre-existing two-segment-then-bare-name pipeline unchanged. `Some("")` =
/// tracked but the receiver is a builtin or unresolved type — no lookup is
/// attempted at all; the edge must stay unbound. `Some(ty)` = tracked with
/// an inferred receiver type — a match on `ty`'s own method is tried first;
/// if `ty` declares no such method, its recorded EXTENDS/IMPLEMENTS/INHERITS
/// ancestors are walked for the one that does (see
/// `resolve_via_inheritance`). No bare-name fallback either way: an
/// unmatched but known-typed receiver stays unbound rather than guessing.
///
/// Returns `(target_symbol_id, resolution_kind)`, where `resolution_kind`
/// is one of `"receiver_type"`, `"inherited"`, `"two_segment"`,
/// `"bare_name"`, or `None` when nothing binds. Exact-qualname resolution
/// (`"exact"`) happens separately, before this is called — see callers.
#[allow(clippy::too_many_arguments)]
fn resolve_fuzzy_target(
    target_qualname: &str,
    receiver_type: Option<&str>,
    edge_kind: &str,
    source_lang: &str,
    graph_version: i64,
    same_lang_stmt: &mut rusqlite::Statement<'_>,
    any_lang_stmt: &mut rusqlite::Statement<'_>,
    hierarchy_stmt: &mut rusqlite::Statement<'_>,
) -> rusqlite::Result<(Option<i64>, Option<&'static str>)> {
    let source_lang = resolution_language_family(source_lang);
    match receiver_type {
        // Tracked, but the receiver is a builtin/unresolved type: per the
        // resolution rule, must not bind — not even via exact-looking
        // patterns. No query at all.
        Some("") => Ok((None, None)),

        // Tracked with a known receiver type: the *only* tier tried is a
        // suffix match on `{type}.{method}` — reusing the same two-segment
        // pattern machinery, just seeded from the inferred type instead of
        // the call site's literal text. No bare-name fallback: an unmatched
        // known type must stay unbound rather than guess.
        Some(known_type) => {
            let method = qualname_trailing_name(target_qualname);
            let seed = format!("{known_type}.{method}");
            let Some((seg, dot_pattern, colons_pattern)) = two_segment_qualname_patterns(&seed)
            else {
                return Ok((None, None));
            };
            let same_lang = single_unambiguous_match(
                same_lang_stmt,
                params![
                    &seg,
                    &dot_pattern,
                    &colons_pattern,
                    graph_version,
                    graph_version,
                    source_lang
                ],
            )?;
            if same_lang.is_some() {
                return Ok((same_lang, Some("receiver_type")));
            }
            if is_bridge_edge_kind(edge_kind) {
                let any_lang = single_unambiguous_match(
                    any_lang_stmt,
                    params![
                        &seg,
                        &dot_pattern,
                        &colons_pattern,
                        graph_version,
                        graph_version
                    ],
                )?;
                if any_lang.is_some() {
                    return Ok((any_lang, Some("receiver_type")));
                }
            }
            // The receiver's own type declares no matching method (or the
            // match there was itself ambiguous) — walk its recorded
            // EXTENDS/IMPLEMENTS/INHERITS ancestors for the one that does.
            resolve_via_inheritance(
                known_type,
                method,
                source_lang,
                edge_kind,
                graph_version,
                same_lang_stmt,
                any_lang_stmt,
                hierarchy_stmt,
            )
        }

        // Not tracked: the pre-existing pipeline, unchanged behavior.
        None => {
            if let Some((two_seg, two_dot, two_colons)) =
                two_segment_qualname_patterns(target_qualname)
            {
                let same_lang = single_unambiguous_match(
                    same_lang_stmt,
                    params![
                        &two_seg,
                        &two_dot,
                        &two_colons,
                        graph_version,
                        graph_version,
                        source_lang
                    ],
                )?;
                if same_lang.is_some() {
                    return Ok((same_lang, Some("two_segment")));
                }
                if is_bridge_edge_kind(edge_kind) {
                    let any_lang = single_unambiguous_match(
                        any_lang_stmt,
                        params![
                            &two_seg,
                            &two_dot,
                            &two_colons,
                            graph_version,
                            graph_version
                        ],
                    )?;
                    if any_lang.is_some() {
                        return Ok((any_lang, Some("two_segment")));
                    }
                }
            }

            let (method_name, dot_pattern, colons_pattern) =
                fuzzy_qualname_patterns(target_qualname);
            let same_lang = single_unambiguous_match(
                same_lang_stmt,
                params![
                    method_name,
                    &dot_pattern,
                    &colons_pattern,
                    graph_version,
                    graph_version,
                    source_lang
                ],
            )?;
            if same_lang.is_some() {
                return Ok((same_lang, Some("bare_name")));
            }
            if is_bridge_edge_kind(edge_kind) {
                let any_lang = single_unambiguous_match(
                    any_lang_stmt,
                    params![
                        method_name,
                        &dot_pattern,
                        &colons_pattern,
                        graph_version,
                        graph_version
                    ],
                )?;
                if any_lang.is_some() {
                    return Ok((any_lang, Some("bare_name")));
                }
            }
            Ok((None, None))
        }
    }
}

/// Maximum number of EXTENDS/IMPLEMENTS/INHERITS hops `resolve_via_inheritance`
/// will follow from a receiver's own type before giving up. Real class
/// hierarchies are rarely more than a handful of levels deep; this bound
/// exists purely so a cyclic or pathological hierarchy graph can't turn one
/// unresolved call into unbounded work during indexing.
///
/// ponytail: a flat hop cap, not cycle detection — `seen` (below) still
/// dedupes symbols already visited so a cycle can't be walked twice, but the
/// cap is what actually bounds worst-case cost. 8 is comfortably past any
/// hierarchy depth seen in real corpora (dpb tops out well under this).
const MAX_INHERITANCE_DEPTH: usize = 8;

/// Resolve a bare type name (e.g. a receiver's inferred `ReceiverType::Known`
/// value, or an ancestor's `target_qualname` text) to the single symbol that
/// declares it. Reuses the same statement `resolve_fuzzy_target` already
/// binds method names against — its kind filter already includes
/// class/interface/struct/trait/record — just seeded with a single-segment
/// suffix pattern instead of a two-segment one. Ambiguity-guarded like every
/// other fuzzy tier: more than one same-named type is unresolvable, not a
/// coin flip. Same-language only — a class hierarchy never crosses a
/// language boundary, so there is no cross-language fallback to attempt.
fn resolve_type_symbol(
    type_name: &str,
    source_lang: &str,
    graph_version: i64,
    same_lang_stmt: &mut rusqlite::Statement<'_>,
) -> rusqlite::Result<Option<i64>> {
    let (name, dot_pattern, colons_pattern) = fuzzy_qualname_patterns(type_name);
    single_unambiguous_match(
        same_lang_stmt,
        params![
            name,
            &dot_pattern,
            &colons_pattern,
            graph_version,
            graph_version,
            source_lang
        ],
    )
}

/// When a receiver's own type declares no matching method, walk up its
/// recorded EXTENDS/IMPLEMENTS/INHERITS edges (`hierarchy_stmt`) for the
/// ancestor that actually declares it — the base-class/interface method a
/// call through that receiver would dispatch to.
///
/// Ancestors are visited breadth-first, level by level, in the declaration
/// order their edges were recorded in (`hierarchy_stmt`'s `ORDER BY id
/// ASC`). Within one level, "first declared, first checked" — if exactly one
/// ancestor at that level declares the method, that is the bind target and
/// the walk stops there without looking deeper (a closer ancestor always
/// wins over a farther one, matching real dispatch). If more than one
/// ancestor at the *same* level declares it, that is a genuine ambiguity
/// (C# multiple interfaces, Python multiple bases) and the call is refused,
/// same as the existing bare-name ambiguity guard — we do not guess which
/// one the language would actually pick. Bounded by
/// `MAX_INHERITANCE_DEPTH`; a walk that exhausts its budget without a match
/// (or without discovering any ancestors to descend into) returns `None`,
/// same as "not found" anywhere else in this pipeline.
///
/// Only called after the receiver's own type has already been checked and
/// missed — see the `Some(known_type)` arm of `resolve_fuzzy_target`.
#[allow(clippy::too_many_arguments)]
fn resolve_via_inheritance(
    known_type: &str,
    method: &str,
    source_lang: &str,
    edge_kind: &str,
    graph_version: i64,
    same_lang_stmt: &mut rusqlite::Statement<'_>,
    any_lang_stmt: &mut rusqlite::Statement<'_>,
    hierarchy_stmt: &mut rusqlite::Statement<'_>,
) -> rusqlite::Result<(Option<i64>, Option<&'static str>)> {
    let Some(root_id) =
        resolve_type_symbol(known_type, source_lang, graph_version, same_lang_stmt)?
    else {
        return Ok((None, None));
    };

    let mut frontier = vec![root_id];
    let mut seen: std::collections::HashSet<i64> = std::collections::HashSet::from([root_id]);

    for _ in 0..MAX_INHERITANCE_DEPTH {
        // Collect this level's direct ancestors, in declaration order,
        // across every symbol reached at the previous level.
        let mut level: Vec<(Option<i64>, String)> = Vec::new();
        for &sym_id in &frontier {
            let rows = hierarchy_stmt.query_map(params![sym_id, graph_version], |row| {
                Ok((row.get::<_, Option<i64>>(0)?, row.get::<_, String>(1)?))
            })?;
            for row in rows {
                level.push(row?);
            }
        }
        if level.is_empty() {
            break;
        }

        // Does any ancestor at this level declare the method?
        let mut matches: Vec<i64> = Vec::new();
        for (_, ancestor_qualname) in &level {
            let ancestor_name = qualname_trailing_name(ancestor_qualname);
            let seed = format!("{ancestor_name}.{method}");
            let Some((seg, dot_pattern, colons_pattern)) = two_segment_qualname_patterns(&seed)
            else {
                continue;
            };
            let same_lang = single_unambiguous_match(
                same_lang_stmt,
                params![
                    &seg,
                    &dot_pattern,
                    &colons_pattern,
                    graph_version,
                    graph_version,
                    source_lang
                ],
            )?;
            let found = if same_lang.is_some() {
                same_lang
            } else if is_bridge_edge_kind(edge_kind) {
                single_unambiguous_match(
                    any_lang_stmt,
                    params![
                        &seg,
                        &dot_pattern,
                        &colons_pattern,
                        graph_version,
                        graph_version
                    ],
                )?
            } else {
                None
            };
            if let Some(id) = found {
                matches.push(id);
            }
        }

        match matches.len() {
            0 => {}
            1 => return Ok((Some(matches[0]), Some("inherited"))),
            _ => return Ok((None, None)), // two unrelated ancestors both declare it: refuse
        }

        // Nobody at this level declares it — descend to the next level.
        // Prefer each ancestor's already-resolved target_symbol_id (cheap,
        // and already vetted by its own EXTENDS/IMPLEMENTS resolution);
        // only re-resolve by name when it's still NULL, e.g. within the
        // same insert_edges pass, before the ancestor's own defining file
        // has been processed.
        let mut next_frontier = Vec::new();
        for (ancestor_symbol_id, ancestor_qualname) in &level {
            let next_id = match ancestor_symbol_id {
                Some(id) => Some(*id),
                None => resolve_type_symbol(
                    qualname_trailing_name(ancestor_qualname),
                    source_lang,
                    graph_version,
                    same_lang_stmt,
                )?,
            };
            if let Some(id) = next_id
                && seen.insert(id)
            {
                next_frontier.push(id);
            }
        }
        if next_frontier.is_empty() {
            break;
        }
        frontier = next_frontier;
    }

    Ok((None, None))
}

fn resolve_symbol_id(
    qualname: &Option<String>,
    symbol_map: &HashMap<String, i64>,
    stmt: &mut rusqlite::Statement<'_>,
    graph_version: i64,
) -> Result<Option<i64>> {
    let name = match qualname.as_ref() {
        Some(name) => name,
        None => return Ok(None),
    };
    if let Some(id) = symbol_map.get(name) {
        return Ok(Some(*id));
    }
    let id = stmt
        .query_row(params![name, graph_version], |row| row.get(0))
        .optional()?;
    Ok(id)
}

/// Encode `EdgeInput::import_candidates` for the `edges.import_candidates`
/// column: `None` (stored as SQL NULL) when the extractor produced no
/// candidates for this edge — the overwhelming common case, since only a
/// dotted `X.method()` call whose receiver import-resolves produces any —
/// so `resolve_null_target_edges` can select "rows worth retrying via the
/// import tier" with a cheap `IS NOT NULL` instead of parsing every row.
/// `Some(json)` otherwise, a plain JSON array of strings.
fn encode_import_candidates(candidates: &[String]) -> Option<String> {
    if candidates.is_empty() {
        None
    } else {
        // A `Vec<String>` always serializes; the `unwrap_or(None)` is only
        // to avoid a panic path in this DB-write hot loop, not because
        // failure is expected.
        serde_json::to_string(candidates).ok()
    }
}

/// Inverse of `encode_import_candidates`, for `resolve_null_target_edges`'s
/// import-candidate retry pass. Malformed JSON (shouldn't happen — nothing
/// but `encode_import_candidates` ever writes this column) decodes to an
/// empty candidate list rather than failing the whole repair pass.
fn decode_import_candidates(raw: &str) -> Vec<String> {
    serde_json::from_str(raw).unwrap_or_default()
}

/// Resolve a call's import-qualified candidate qualnames (see
/// `EdgeInput::import_candidates` / `csharp::import_qualified_candidates`)
/// against the real symbol table, binding only when precisely one distinct
/// symbol is found across *every* candidate — mirrors the ambiguity guard
/// in `single_unambiguous_match`, just over a short candidate list instead
/// of a SQL suffix pattern. Each candidate is itself an exact-qualname
/// lookup (same map-then-SQL path as `resolve_symbol_id`), so this is
/// authoritative when it hits: a candidate naming a real symbol is never a
/// coincidental substring/suffix match.
///
/// Returns `None` for an empty candidate list (the common case — every
/// extractor except C# leaves it empty, as does most C# calls), for zero
/// hits, and for 2+ *distinct* hits (whether from two different candidates
/// each naming a different real symbol, e.g. two `using`s that both
/// happen to supply a type by this name, or — in principle — one
/// candidate naming more than one symbol). Either way, the caller falls
/// through to the pre-existing exact/two-segment/bare-name tiers
/// unchanged, so this never bypasses the ambiguity guard, only sometimes
/// avoids tripping it by qualifying an otherwise-ambiguous receiver first.
///
/// Only when *no* candidate hits exactly, a second round retries each one
/// as a dotted-path suffix (`suffix_stmt`, `IMPORT_SUFFIX_LOOKUP_SQL`), same
/// one-distinct-hit rule. Needed for Python, where a file's module qualname
/// is its repo-relative path (`py.pkg.src.pkg.mod`) while the import names
/// the installed package path (`pkg.mod`), so the exact round never hits
/// for a src-layout repo. Still the full import path, never a bare name.
fn resolve_import_candidate(
    candidates: &[String],
    symbol_map: &HashMap<String, i64>,
    stmt: &mut rusqlite::Statement<'_>,
    suffix_stmt: &mut rusqlite::Statement<'_>,
    graph_version: i64,
) -> Result<Option<i64>> {
    for exact_round in [true, false] {
        let mut found: Option<i64> = None;
        for candidate in candidates {
            let id = if !exact_round {
                let name = candidate.rsplit('.').next().unwrap_or(candidate);
                let suffix = format!(".{candidate}");
                single_unambiguous_match(suffix_stmt, params![name, suffix, graph_version])?
            } else if let Some(&id) = symbol_map.get(candidate) {
                Some(id)
            } else {
                stmt.query_row(params![candidate, graph_version], |row| row.get(0))
                    .optional()?
            };
            let Some(id) = id else { continue };
            match found {
                None => found = Some(id),
                Some(existing) if existing == id => {}
                Some(_) => return Ok(None),
            }
        }
        if found.is_some() {
            return Ok(found);
        }
    }
    Ok(None)
}

/// Suffix round of `resolve_import_candidate`: params are (trailing name,
/// `.{candidate}`, graph_version). `substr(.., -n)` is an exact tail
/// comparison, so `_`/`%` in names are not LIKE wildcards. `LIMIT 2` feeds
/// `single_unambiguous_match`'s ambiguity guard.
const IMPORT_SUFFIX_LOOKUP_SQL: &str = "SELECT id FROM symbols
     WHERE name = ?1 AND substr(qualname, -length(?2)) = ?2 AND graph_version = ?3
     LIMIT 2";

/// Whether a Python edge's unresolved import candidates point into this
/// repo: a relative import (`.mod.x`, leading dot), or one whose root
/// package has a Python `module` symbol here (`stmt`, any graph version so
/// an incremental reindex that has not yet carried the package forward
/// still counts it). When false, the import is external (stdlib,
/// third-party) and must shadow the bare name — see the guard in
/// `insert_edges`.
///
/// ponytail: root-name only, so a repo submodule sharing a stdlib root
/// name (`pkg.common.logging` vs `import logging`) makes that stdlib
/// import look repo-local and keeps today's fuzzy behavior for it. Upgrade
/// path: match the import's full module path, not just its root.
///
/// ponytail: a `_pb2`/`_pb2_grpc` segment is protoc output, never checked
/// in, so it counts as external even under a repo package — otherwise
/// `pb.ColumnDef(...)` from `from pkg.v1 import pkg_pb2 as pb` fuzzy-binds
/// to a same-named repo dataclass (41 such edges in dpb). Other gitignored
/// generated modules still slip through; upgrade path: check the repo
/// module's own bindings for the next segment.
fn is_repo_python_import(
    candidates: &[String],
    stmt: &mut rusqlite::Statement<'_>,
) -> Result<bool> {
    for candidate in candidates {
        if candidate
            .split('.')
            .any(|seg| seg.ends_with("_pb2") || seg.ends_with("_pb2_grpc"))
        {
            continue;
        }
        let root = candidate.split('.').next().unwrap_or("");
        if root.is_empty() || stmt.exists(params![root])? {
            return Ok(true);
        }
    }
    Ok(false)
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

    fn make_test_edge(
        kind: &str,
        source_qualname: &str,
        target_qualname: &str,
    ) -> crate::indexer::extract::EdgeInput {
        make_test_edge_with_receiver_type(
            kind,
            source_qualname,
            target_qualname,
            ReceiverType::NotTracked,
        )
    }

    fn make_test_edge_with_receiver_type(
        kind: &str,
        source_qualname: &str,
        target_qualname: &str,
        receiver_type: ReceiverType,
    ) -> crate::indexer::extract::EdgeInput {
        crate::indexer::extract::EdgeInput {
            kind: kind.to_string(),
            source_qualname: Some(source_qualname.to_string()),
            target_qualname: Some(target_qualname.to_string()),
            detail: None,
            evidence_snippet: None,
            evidence_start_line: None,
            evidence_end_line: None,
            confidence: Some(1.0),
            trace_id: None,
            span_id: None,
            event_ts: None,
            receiver_type,
            import_candidates: Vec::new(),
        }
    }

    fn make_test_edge_with_import_candidates(
        kind: &str,
        source_qualname: &str,
        target_qualname: &str,
        import_candidates: Vec<String>,
    ) -> crate::indexer::extract::EdgeInput {
        crate::indexer::extract::EdgeInput {
            import_candidates,
            ..make_test_edge_with_receiver_type(
                kind,
                source_qualname,
                target_qualname,
                ReceiverType::NotTracked,
            )
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
    fn test_database_migration_adds_receiver_type_and_resolution_kind_columns() {
        let (db, _temp) = create_test_db();

        let conn = db.read_conn().unwrap();
        let mut stmt = conn.prepare("PRAGMA table_info(edges)").unwrap();
        let columns: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert!(
            columns.contains(&"receiver_type".to_string()),
            "edges table should have receiver_type column after migration"
        );
        assert!(
            columns.contains(&"resolution_kind".to_string()),
            "edges table should have resolution_kind column after migration"
        );
        // confidence must be untouched by this migration — it keeps its
        // pre-existing meaning (Rust CALLS extraction certainty).
        assert!(columns.contains(&"confidence".to_string()));
    }

    /// Column names for `table`, read straight from the live schema via
    /// `PRAGMA table_info`, in table-definition order (which matches
    /// `SELECT *`'s column order — used below to locate each column's value
    /// positionally).
    fn table_columns(db: &Db, table: &str) -> Vec<String> {
        let conn = db.conn();
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .unwrap();
        stmt.query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    }

    /// Regression guard for the whole *class* of bug behind the
    /// `receiver_type`/`resolution_kind` data-loss fix above (and, before
    /// that, `symbol_metrics` being dropped wholesale): `carry_forward_files`
    /// names its copied columns explicitly in Rust-side SQL, so a column
    /// added to `symbols`/`edges`/`symbol_metrics` after the fact is silently
    /// NOT copied unless someone remembers to also update this unrelated
    /// function.
    ///
    /// Rather than hardcoding a second column list here (which could drift
    /// out of sync with the schema exactly the way the SQL itself did), this
    /// stamps a unique sentinel into every column `PRAGMA table_info` reports
    /// for each table — except a small, explicit, commented allowlist — runs
    /// a real carry-forward, and asserts every one of those columns' values
    /// survived onto the new graph version. A newly added column that
    /// `carry_forward_files` doesn't copy comes back NULL and fails loudly,
    /// naming exactly the table and column at fault.
    #[test]
    fn carry_forward_files_copies_every_non_exempt_column() {
        // Columns intentionally excluded from the generic sentinel-and-verify
        // sweep below. Each entry is exempt for a specific, different reason
        // -- none of them are "silently dropped", they just aren't a literal
        // same-value copy, so a sentinel round-trip check doesn't apply.
        let exempt: &[(&str, &[&str])] = &[
            (
                "symbols",
                &[
                    // INTEGER PRIMARY KEY: every copy gets a fresh autoincrement
                    // id by design (that's how the "old" and "new" rows stay
                    // distinguishable at all).
                    "id",
                    // Set to `to_version` by the copy itself -- carrying a file
                    // "forward" to a new version *is* changing this column.
                    "graph_version",
                    // Used verbatim in the copy's own `WHERE file_id IN (...)`
                    // filter; stamping it with a sentinel would stop the source
                    // row from being selected at all instead of exercising the
                    // bug. It's still carried forward unchanged (`SELECT
                    // file_id`) -- checked by the real-value `file_id`
                    // assertion below instead of the generic sentinel sweep.
                    "file_id",
                ],
            ),
            (
                "edges",
                &[
                    "id",
                    "graph_version",
                    // Same reasoning as symbols.file_id above: used in this
                    // copy's own `WHERE e.file_id IN (...)` filter.
                    "file_id",
                    // Remapped via a `stable_id` lookup into the new version's
                    // symbols (see the "ponytail" comment on
                    // `carry_forward_files`), not a literal copy of the old
                    // id -- an endpoint with no match is intentionally carried
                    // as NULL. Binding correctness for these is covered by the
                    // dangling-edges query elsewhere, not this test.
                    "source_symbol_id",
                    "target_symbol_id",
                ],
            ),
            (
                "symbol_metrics",
                &[
                    "id",
                    // Same stable_id-based remap as edges' endpoints above.
                    "symbol_id",
                    // `FOREIGN KEY(file_id) REFERENCES files(id)`: a sentinel
                    // string here would fail that constraint (foreign keys
                    // are on for every connection, see `Db::new`), unlike
                    // symbols/edges' unconstrained `file_id`. It's still
                    // carried forward unchanged (`sm.file_id`) -- checked by
                    // the real-value `file_id` assertion below instead of the
                    // generic sentinel sweep.
                    "file_id",
                ],
            ),
        ];
        let is_exempt = |table: &str, column: &str| {
            exempt
                .iter()
                .find(|(t, _)| *t == table)
                .map(|(_, cols)| cols.contains(&column))
                .unwrap_or(false)
        };

        let (mut db, _temp) = create_test_db();

        let file_id = db
            .upsert_file("carry_guard.py", "hash1", "python", 10, 0)
            .unwrap();

        let symbols = vec![make_test_symbol(
            "carry_guard.fn",
            Some("()"),
            "function",
            1,
        )];
        let inserted = db
            .insert_symbols(file_id, "carry_guard.py", &symbols, 1, Some("sha1"))
            .unwrap();
        let symbol_id = inserted[0].id;
        let mut symbol_map = HashMap::new();
        symbol_map.insert("carry_guard.fn".to_string(), symbol_id);

        // Self-referential CALLS edge: only the DB wiring matters here, not
        // realistic call semantics.
        let edges = vec![make_test_edge_with_receiver_type(
            "CALLS",
            "carry_guard.fn",
            "carry_guard.fn",
            ReceiverType::Known("Foo".to_string()),
        )];
        db.insert_edges(file_id, &edges, &symbol_map, 1, Some("sha1"))
            .unwrap();

        let metrics = vec![SymbolMetricsInput {
            qualname: "carry_guard.fn".to_string(),
            loc: 5,
            complexity: 2,
            duplication_hash: Some("duphash".to_string()),
        }];
        db.insert_symbol_metrics(file_id, &metrics, &symbol_map)
            .unwrap();

        // Stamp a unique, non-NULL sentinel into every non-exempt column of
        // the one row on each table, so a column `carry_forward_files`
        // silently drops comes back NULL instead of "not obviously wrong".
        for table in ["symbols", "edges", "symbol_metrics"] {
            for column in table_columns(&db, table) {
                if is_exempt(table, &column) {
                    continue;
                }
                let sentinel = format!("cf_guard::{table}::{column}");
                db.conn()
                    .execute(
                        &format!("UPDATE {table} SET {column} = ?1"),
                        params![sentinel],
                    )
                    .unwrap_or_else(|e| panic!("seeding sentinel for {table}.{column}: {e}"));
            }
        }

        db.carry_forward_files(&[file_id], 1, 2).unwrap();

        let new_symbol_id: i64 = db
            .conn()
            .query_row(
                "SELECT id FROM symbols WHERE file_id = ?1 AND graph_version = 2",
                params![file_id],
                |row| row.get(0),
            )
            .expect("carry_forward_files must copy the symbols row to the new graph version");
        let new_edge_id: i64 = db
            .conn()
            .query_row(
                "SELECT id FROM edges WHERE file_id = ?1 AND graph_version = 2",
                params![file_id],
                |row| row.get(0),
            )
            .expect("carry_forward_files must copy the edges row to the new graph version");
        let new_metrics_id: i64 = db
            .conn()
            .query_row(
                "SELECT id FROM symbol_metrics WHERE symbol_id = ?1",
                params![new_symbol_id],
                |row| row.get(0),
            )
            .expect(
                "carry_forward_files must copy the symbol_metrics row to the new graph version",
            );

        let new_row_ids: &[(&str, i64)] = &[
            ("symbols", new_symbol_id),
            ("edges", new_edge_id),
            ("symbol_metrics", new_metrics_id),
        ];

        for (table, row_id) in new_row_ids {
            for column in table_columns(&db, table) {
                if is_exempt(table, &column) {
                    continue;
                }
                let expected = rusqlite::types::Value::Text(format!("cf_guard::{table}::{column}"));
                let actual: rusqlite::types::Value = db
                    .conn()
                    .query_row(
                        &format!("SELECT {column} FROM {table} WHERE id = ?1"),
                        params![row_id],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert_eq!(
                    actual, expected,
                    "carry_forward_files did not copy `{table}.{column}` into the new graph \
                     version (found {actual:?}, expected the source row's value {expected:?}). \
                     Add `{column}` to both the INSERT column list and the SELECT in \
                     Db::carry_forward_files's `{table}` copy -- or, if `{column}` must \
                     genuinely never be carried forward, add it to this test's `exempt` list \
                     with a comment explaining why."
                );
            }
        }

        // `file_id` is excluded from the generic sweep above on all three
        // tables (see the `exempt` comments), but it must still survive the
        // copy unchanged -- check it directly against the real value instead
        // of a sentinel.
        for (table, row_id) in new_row_ids {
            let actual_file_id: i64 = db
                .conn()
                .query_row(
                    &format!("SELECT file_id FROM {table} WHERE id = ?1"),
                    params![row_id],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(
                actual_file_id, file_id,
                "carry_forward_files did not preserve `{table}.file_id` on the copied row"
            );
        }
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
            receiver_type: crate::indexer::extract::ReceiverType::NotTracked,
            import_candidates: Vec::new(),
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
        // `incoming_edges_by_qualname_pattern` used to re-attribute edges by a
        // `target_qualname LIKE '%.{bare_name}'` suffix match — i.e. it invented
        // an attribution for whatever the read path was showing, independent of
        // what the write path had actually resolved. That's issue #45 (see also
        // the `edges_for_symbols` fix in graph_query.rs for the same class of
        // bug): the function is now permanently neutered to return no edges, so
        // this asserts the new (empty) contract rather than the old resurrection
        // behavior. `test_edges_for_symbols_does_not_resurrect_null_target_edge`
        // and `test_edges_for_symbols_case_differing_name_in_another_language_does_not_match`
        // below cover the underlying guiding principle with a non-vacuous setup.
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
            receiver_type: crate::indexer::extract::ReceiverType::NotTracked,
            import_candidates: Vec::new(),
        }];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        // Even though a matching, already-*resolved* edge exists (mod.Caller ->
        // mod.Callee, bound via insert_edges' own exact-match tier), the
        // function must return nothing: it no longer performs qualname-pattern
        // matching at all.
        let found = db
            .incoming_edges_by_qualname_pattern("Callee", "CALLS", None, 1)
            .unwrap();
        assert!(found.is_empty());
    }

    #[test]
    fn test_edges_for_symbols_does_not_resurrect_null_target_edge() {
        // Regression for the read-path resurrection bug: a receiver-qualified
        // call (`buf.append(x)`) whose receiver type isn't in scope has two
        // same-language, same-bare-name candidates, so it's genuinely
        // ambiguous and the write path (insert_edges' ambiguity guard)
        // deliberately leaves it unresolved. Neither candidate symbol must
        // see it as an incoming call. The target is deliberately dotted
        // ("buf.append", not bare "append") so this exercises the exact
        // `target_qualname.ends_with(".{name}")` suffix match the old
        // `edges_for_symbols` second query used to resurrect through.
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("pkg/store.py", "h1", "python", 100, 0)
            .unwrap();
        let symbols = vec![
            make_test_symbol("builtins.list.append", Some("def append(x)"), "method", 1),
            make_test_symbol(
                "pkg.store.EventStore.append",
                Some("def append(self, event)"),
                "method",
                10,
            ),
            make_test_symbol("pkg.store.caller", Some("def caller()"), "function", 20),
        ];
        let inserted = db
            .insert_symbols(file_id, "pkg/store.py", &symbols, 1, None)
            .unwrap();
        let list_append_id = inserted
            .iter()
            .find(|s| s.qualname == "builtins.list.append")
            .unwrap()
            .id;
        let event_store_append_id = inserted
            .iter()
            .find(|s| s.qualname == "pkg.store.EventStore.append")
            .unwrap()
            .id;

        let edges = vec![make_test_edge("CALLS", "pkg.store.caller", "buf.append")];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        // Confirm the edge really is unresolved (the write path refused it).
        let unresolved_count: i64 = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE target_symbol_id IS NULL AND graph_version = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unresolved_count, 1);

        let by_symbol = db
            .edges_for_symbols(&[list_append_id, event_store_append_id], None, 1)
            .unwrap();
        assert!(
            by_symbol[&list_append_id].is_empty(),
            "ambiguous unresolved edge must not be attributed to list.append"
        );
        assert!(
            by_symbol[&event_store_append_id].is_empty(),
            "ambiguous unresolved edge must not be attributed to EventStore.append either"
        );
    }

    #[test]
    fn test_edges_for_symbols_case_differing_name_in_another_language_does_not_match() {
        // Regression for the exact bug the user hit: a C# `value.Trim()` call
        // (receiver type unresolved, so the write path correctly leaves it
        // NULL) must never be shown as a callee/caller of an unrelated Python
        // `trim` function just because SQLite's LIKE is case-insensitive for
        // ASCII (`'value.Trim' LIKE '%.trim'`).
        let (mut db, _temp) = create_test_db();
        let cs_file = db
            .upsert_file("UniqueName.cs", "h1", "csharp", 100, 0)
            .unwrap();
        let py_file = db
            .upsert_file("functions.py", "h2", "python", 100, 0)
            .unwrap();

        let cs_symbols = vec![make_test_symbol(
            "Dpb.UniqueName.Create",
            Some("static Create(string value)"),
            "method",
            1,
        )];
        let cs_inserted = db
            .insert_symbols(cs_file, "UniqueName.cs", &cs_symbols, 1, None)
            .unwrap();

        let py_symbols = vec![make_test_symbol(
            "py.dpbuilder.functions.trim",
            Some("def trim(e)"),
            "function",
            1,
        )];
        let py_inserted = db
            .insert_symbols(py_file, "functions.py", &py_symbols, 1, None)
            .unwrap();
        let py_trim_id = py_inserted[0].id;

        // receiver_type: Unresolved -- tracked but the receiver's type
        // (`value`, a local `string?`) could not be determined, so resolution
        // must not bind this edge at all (exact match also can't hit: no
        // symbol is named exactly "value.Trim").
        let edges = vec![make_test_edge_with_receiver_type(
            "CALLS",
            "Dpb.UniqueName.Create",
            "value.Trim",
            ReceiverType::Unresolved,
        )];
        let symbol_map: HashMap<String, i64> = cs_inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(cs_file, &edges, &symbol_map, 1, None)
            .unwrap();

        // Confirm the edge is unresolved before exercising the read path.
        let target_symbol_id: Option<i64> = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT target_symbol_id FROM edges WHERE target_qualname = 'value.Trim'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(target_symbol_id, None);

        let by_symbol = db.edges_for_symbols(&[py_trim_id], None, 1).unwrap();
        assert!(
            by_symbol[&py_trim_id].is_empty(),
            "C# value.Trim() must not resurrect as a caller of Python trim()"
        );

        let incoming = db
            .incoming_edges_by_qualname_pattern("trim", "CALLS", None, 1)
            .unwrap();
        assert!(incoming.is_empty());
    }

    #[test]
    fn test_edges_for_symbols_still_shows_genuinely_resolved_edge() {
        // Sanity check that the fix above didn't throw out the happy path:
        // an edge the write path *did* resolve must still show up.
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![
            make_test_symbol("mod.Caller", Some("fn caller()"), "function", 1),
            make_test_symbol("mod.Callee", Some("fn callee()"), "function", 10),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();
        let caller_id = inserted[0].id;
        let callee_id = inserted[1].id;

        let edges = vec![make_test_edge("CALLS", "mod.Caller", "mod.Callee")];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let by_symbol = db
            .edges_for_symbols(&[caller_id, callee_id], None, 1)
            .unwrap();
        assert_eq!(by_symbol[&caller_id].len(), 1);
        assert_eq!(by_symbol[&caller_id][0].target_symbol_id, Some(callee_id));
        assert_eq!(by_symbol[&callee_id].len(), 1);
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
            receiver_type: crate::indexer::extract::ReceiverType::NotTracked,
            import_candidates: Vec::new(),
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
                receiver_type: crate::indexer::extract::ReceiverType::NotTracked,
                import_candidates: Vec::new(),
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
                receiver_type: crate::indexer::extract::ReceiverType::NotTracked,
                import_candidates: Vec::new(),
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
    fn test_edge_lookups_use_selective_index_not_graph_version() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/a.rs", "h1", "rust", 100, 0).unwrap();
        let symbols: Vec<_> = (0..200)
            .map(|i| make_test_symbol(&format!("m.f{i}"), None, "function", i + 1))
            .collect();
        let inserted = db
            .insert_symbols(file_id, "src/a.rs", &symbols, 1, None)
            .unwrap();
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        let edges: Vec<_> = (0..199)
            .map(|i| crate::indexer::extract::EdgeInput {
                kind: "CALLS".to_string(),
                source_qualname: Some(format!("m.f{i}")),
                target_qualname: Some(format!("m.f{}", i + 1)),
                ..Default::default()
            })
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();
        let plan: String = db
            .read_conn()
            .unwrap()
            .query_row(
                "EXPLAIN QUERY PLAN SELECT id FROM edges
                 WHERE target_symbol_id = 5 AND graph_version = 1",
                [],
                |row| row.get(3),
            )
            .unwrap();
        assert!(plan.contains("idx_edges_target"), "{plan}");
    }

    #[test]
    fn test_migration_15_drops_graph_version_indexes_from_existing_db() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("old.db");
        drop(Db::new(&path).unwrap());
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE INDEX idx_edges_graph_version ON edges(graph_version);
             UPDATE meta SET value = '14' WHERE key = 'schema_version';",
        )
        .unwrap();
        drop(conn);
        drop(Db::new(&path).unwrap());
        let left: i64 = Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE name LIKE 'idx_%graph_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(left, 0);
    }

    #[test]
    fn test_rpc_bridge_requires_real_route_when_protos_indexed() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/Svc.cs", "h1", "csharp", 100, 0)
            .unwrap();
        let symbols = vec![
            make_test_symbol("A.Test", None, "method", 1),
            make_test_symbol("B.Impl.Deploy", None, "method", 10),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/Svc.cs", &symbols, 1, None)
            .unwrap();
        let edge = |kind: &str, src: Option<&str>, tq: &str| crate::indexer::extract::EdgeInput {
            kind: kind.to_string(),
            source_qualname: src.map(str::to_string),
            target_qualname: Some(tq.to_string()),
            ..Default::default()
        };
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        // Both sides guessed the same bogus package; no .proto defines it.
        let guessed = "/dpb.datamgr.deployerservice/deploy";
        db.insert_edges(
            file_id,
            &[
                edge("RPC_CALL", Some("A.Test"), guessed),
                edge("RPC_IMPL", Some("B.Impl.Deploy"), guessed),
            ],
            &symbol_map,
            1,
            None,
        )
        .unwrap();
        // No RPC_ROUTE anywhere yet: unguarded, the pair still bridges.
        let found = db
            .edges_by_target_qualname_and_kinds(guessed, &["RPC_IMPL"], None, 1)
            .unwrap();
        assert_eq!(found.len(), 1);

        // Once any real route is indexed, an unbacked path no longer bridges.
        let proto_id = db
            .upsert_file("protos/d.proto", "h2", "proto", 10, 0)
            .unwrap();
        let real = "/datasource.deployer.v1.deployerservice/deploy";
        db.insert_edges(
            proto_id,
            &[edge("RPC_ROUTE", None, real)],
            &HashMap::new(),
            1,
            None,
        )
        .unwrap();
        let found = db
            .edges_by_target_qualname_and_kinds(guessed, &["RPC_IMPL"], None, 1)
            .unwrap();
        assert!(found.is_empty());

        // A route-backed path still bridges.
        let file2 = db
            .upsert_file("src/Real.cs", "h3", "csharp", 100, 0)
            .unwrap();
        db.insert_edges(
            file2,
            &[edge("RPC_IMPL", Some("B.Impl.Deploy"), real)],
            &symbol_map,
            1,
            None,
        )
        .unwrap();
        let found = db
            .edges_by_target_qualname_and_kinds(real, &["RPC_IMPL"], None, 1)
            .unwrap();
        assert_eq!(found.len(), 1);
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
            receiver_type: crate::indexer::extract::ReceiverType::NotTracked,
            import_candidates: Vec::new(),
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
                receiver_type: crate::indexer::extract::ReceiverType::NotTracked,
                import_candidates: Vec::new(),
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
                receiver_type: crate::indexer::extract::ReceiverType::NotTracked,
                import_candidates: Vec::new(),
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

    // --- qualname_trailing_name helper ---

    #[test]
    fn test_qualname_trailing_name() {
        let cases: &[(&str, &str)] = &[
            // '.' separator
            ("a.b.process", "process"),
            ("_svc.DeployAsync", "DeployAsync"),
            // '::' separator
            ("crate::util::helper::process", "process"),
            ("foo::bar", "bar"),
            // no separator
            ("process", "process"),
            // mixed separators: last one wins
            ("crate::Foo.method", "method"),
            ("pkg.module::func", "func"),
            // trailing separators yield an empty name; downstream patterns
            // ('', '%.', '%::') cannot match any real qualname
            ("foo.", ""),
            ("foo::", ""),
            // leading separators are stripped
            (".foo", "foo"),
            ("::foo", "foo"),
            // a lone ':' (not '::') is part of the name, never a split point
            ("label:name", "label:name"),
            ("a::b:c", "b:c"),
            (":", ":"),
            // degenerate inputs
            ("", ""),
            (".", ""),
            ("::", ""),
            // repeated separators collapse to the last one
            ("a..b", "b"),
            ("a:::b", "b"),
        ];
        for (input, expected) in cases {
            assert_eq!(qualname_trailing_name(input), *expected, "input: {input:?}");
        }
    }

    // --- lookup_symbol_id_fuzzy with '::' qualnames ---

    #[test]
    fn test_lookup_symbol_id_fuzzy_resolves_rust_colons_qualname() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol(
            "crate::util::helper::process",
            Some("fn process()"),
            "function",
            1,
        )];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Bare name target should match via '%::process' suffix pattern
        let id = db.lookup_symbol_id_fuzzy("process", None, 1).unwrap();
        assert_eq!(id, Some(inserted[0].id));

        // Short '::'-qualified target should match full qualname via suffix pattern
        let id = db
            .lookup_symbol_id_fuzzy("helper::process", None, 1)
            .unwrap();
        assert_eq!(id, Some(inserted[0].id));
    }

    #[test]
    fn test_lookup_symbol_id_fuzzy_partial_name_does_not_match() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol(
            "crate::util::reprocess",
            Some("fn reprocess()"),
            "function",
            1,
        )];
        db.insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // "process" must NOT match "reprocess" — no bare '%process' suffix
        let id = db.lookup_symbol_id_fuzzy("process", None, 1).unwrap();
        assert!(id.is_none());
    }

    #[test]
    fn test_lookup_symbol_id_fuzzy_degenerate_targets_return_none() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let symbols = vec![make_test_symbol(
            "crate::util::process",
            Some("fn process()"),
            "function",
            1,
        )];
        db.insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Trailing separators produce an empty trailing name; the resulting
        // patterns ('', '%.', '%::') must not match any real qualname
        assert_eq!(db.lookup_symbol_id_fuzzy("util::", None, 1).unwrap(), None);
        assert_eq!(db.lookup_symbol_id_fuzzy("util.", None, 1).unwrap(), None);
        assert_eq!(db.lookup_symbol_id_fuzzy("", None, 1).unwrap(), None);

        // A single ':' is not a separator: "Foo:process" keeps the whole
        // string as the name and must NOT resolve to "process"
        assert_eq!(
            db.lookup_symbol_id_fuzzy("Foo:process", None, 1).unwrap(),
            None
        );
    }

    // --- insert_edges must not resolve targets against stale graph versions ---

    #[test]
    fn test_insert_edges_target_only_in_older_graph_version_resolves_to_null() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/gather_context.rs", "h1", "rust", 100, 0)
            .unwrap();

        // graph_version 1: a symbol exists under this qualname (e.g. before the file was
        // reorganized into a submodule).
        let old_symbol = vec![make_test_symbol(
            "crate::gather_context::resolve_seeds",
            Some("fn resolve_seeds()"),
            "function",
            182,
        )];
        let old_inserted = db
            .insert_symbols(file_id, "src/gather_context.rs", &old_symbol, 1, None)
            .unwrap();

        // graph_version 2: the symbol above no longer exists under that qualname in this
        // version (it moved/renamed, or its file was deleted). Only the caller is present.
        let caller_symbol = vec![make_test_symbol(
            "crate::gather_context::gather",
            Some("fn gather()"),
            "function",
            1,
        )];
        let caller_inserted = db
            .insert_symbols(file_id, "src/gather_context.rs", &caller_symbol, 2, None)
            .unwrap();

        // An edge written against graph_version 2 still carries the old target_qualname
        // (this is exactly what a re-run of xref::link_cross_language_refs produces: the
        // extractor found a reference by name, but no current-version symbol matches it).
        let edges = vec![make_test_edge(
            "CALLS",
            "crate::gather_context::gather",
            "crate::gather_context::resolve_seeds",
        )];
        let symbol_map: HashMap<String, i64> = caller_inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 2, None)
            .unwrap();

        let found = db.edges_for_symbol(caller_inserted[0].id, None, 2).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(
            found[0].target_symbol_id, None,
            "target_qualname matches only a graph_version=1 symbol (id {}); it must resolve \
             to NULL rather than that stale row",
            old_inserted[0].id
        );
        // The unresolved qualname is preserved for later re-resolution / display.
        assert_eq!(
            found[0].target_qualname.as_deref(),
            Some("crate::gather_context::resolve_seeds")
        );
    }

    // --- insert_edges fuzzy resolution with '::' qualnames ---

    #[test]
    fn test_insert_edges_fuzzy_resolves_rust_colons_target() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();

        let caller_sym = vec![make_test_symbol(
            "crate::caller::call_helper",
            Some("fn call_helper()"),
            "function",
            1,
        )];
        let callee_sym = vec![make_test_symbol(
            "crate::util::helper::process",
            Some("fn process()"),
            "function",
            10,
        )];
        let caller_inserted = db
            .insert_symbols(file_id, "src/lib.rs", &caller_sym, 1, None)
            .unwrap();
        let callee_inserted = db
            .insert_symbols(file_id, "src/lib.rs", &callee_sym, 1, None)
            .unwrap();

        // Edge with bare-name target — no symbol_map entry for "process"
        let edges = vec![make_test_edge(
            "CALLS",
            "crate::caller::call_helper",
            "process",
        )];
        let symbol_map: HashMap<String, i64> = caller_inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        // The inserted edge should have resolved target_symbol_id
        let found = db.edges_for_symbol(caller_inserted[0].id, None, 1).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].target_symbol_id, Some(callee_inserted[0].id));
    }

    // --- resolve_null_target_edges with '::' qualnames ---

    #[test]
    fn test_resolve_null_target_edges_resolves_rust_colons_qualname() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();

        let caller_sym = vec![make_test_symbol(
            "crate::caller::do_work",
            Some("fn do_work()"),
            "function",
            1,
        )];
        let caller_inserted = db
            .insert_symbols(file_id, "src/lib.rs", &caller_sym, 1, None)
            .unwrap();

        // Insert the edge before the callee symbol exists — so target_symbol_id stays NULL.
        // This simulates out-of-order incremental indexing (caller file indexed before callee file).
        let edges = vec![make_test_edge("CALLS", "crate::caller::do_work", "compute")];
        let symbol_map: HashMap<String, i64> = caller_inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        // Now insert the callee symbol (the callee file is indexed later)
        let callee_sym = vec![make_test_symbol(
            "crate::util::helper::compute",
            Some("fn compute()"),
            "function",
            10,
        )];
        let callee_inserted = db
            .insert_symbols(file_id, "src/lib.rs", &callee_sym, 1, None)
            .unwrap();

        // Confirm edge is unresolved
        let conn = db.read_conn().unwrap();
        let unresolved_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE target_symbol_id IS NULL AND graph_version = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unresolved_count, 1);
        drop(conn);

        // Now run resolve_null_target_edges — should resolve via '%::compute' pattern
        let resolved = db.resolve_null_target_edges(1).unwrap();
        assert!(resolved >= 1);

        // Verify the edge now points to the callee
        let found = db.edges_for_symbol(caller_inserted[0].id, None, 1).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].target_symbol_id, Some(callee_inserted[0].id));
    }

    #[test]
    fn test_resolve_null_target_edges_binds_fully_qualified_exact_match() {
        // Regression for the "fully-qualified target doesn't bind" report
        // (dpb's `Dpb.DataMgr.DataProduct.Domain.UniqueName.Create`): a caller
        // file references the callee by its full qualname *before* the callee
        // file has been indexed (out-of-order incremental indexing), exactly
        // like `test_resolve_null_target_edges_resolves_rust_colons_qualname`
        // above but with the target already fully qualified rather than bare.
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();

        let caller_sym = vec![make_test_symbol(
            "crate::caller::do_work",
            Some("fn do_work()"),
            "function",
            1,
        )];
        let caller_inserted = db
            .insert_symbols(file_id, "src/lib.rs", &caller_sym, 1, None)
            .unwrap();

        // Edge target is the callee's full, exact qualname — not a bare name —
        // but the callee symbol doesn't exist yet.
        let edges = vec![make_test_edge(
            "CALLS",
            "crate::caller::do_work",
            "crate::util::helper::compute",
        )];
        let symbol_map: HashMap<String, i64> = caller_inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        // Now the callee file is indexed.
        let callee_sym = vec![make_test_symbol(
            "crate::util::helper::compute",
            Some("fn compute()"),
            "function",
            10,
        )];
        let callee_inserted = db
            .insert_symbols(file_id, "src/lib.rs", &callee_sym, 1, None)
            .unwrap();

        let resolved = db.resolve_null_target_edges(1).unwrap();
        assert_eq!(
            resolved, 1,
            "exact-match repair pass should bind the fully-qualified target"
        );

        let found = db.edges_for_symbol(caller_inserted[0].id, None, 1).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].target_symbol_id, Some(callee_inserted[0].id));

        let resolution_kind: Option<String> = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT resolution_kind FROM edges WHERE id = ?",
                params![found[0].id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(resolution_kind.as_deref(), Some("exact"));
    }

    #[test]
    fn test_insert_edges_fuzzy_prefers_same_language_over_shorter_cross_language() {
        let (mut db, _temp) = create_test_db();
        let file_id_rs = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();
        let file_id_py = db
            .upsert_file("src/app.py", "h2", "python", 100, 0)
            .unwrap();

        // Python candidate has the shorter qualname — without language
        // preference, "shortest wins" would pick it
        let py_syms = vec![make_test_symbol(
            "m.process",
            Some("def process()"),
            "function",
            1,
        )];
        db.insert_symbols(file_id_py, "src/app.py", &py_syms, 1, None)
            .unwrap();

        let rs_syms = vec![
            make_test_symbol("crate::caller::run", Some("fn run()"), "function", 1),
            make_test_symbol(
                "crate::deeply::nested::util::process",
                Some("fn process()"),
                "function",
                10,
            ),
        ];
        let rs_inserted = db
            .insert_symbols(file_id_rs, "src/lib.rs", &rs_syms, 1, None)
            .unwrap();

        // CALLS is not a bridge kind, so only the same-language pass applies;
        // the '::' pattern must find the Rust symbol within that pass
        let edges = vec![make_test_edge("CALLS", "crate::caller::run", "process")];
        let symbol_map: HashMap<String, i64> = rs_inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id_rs, &edges, &symbol_map, 1, None)
            .unwrap();

        let found = db.edges_for_symbol(rs_inserted[0].id, None, 1).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].target_symbol_id, Some(rs_inserted[1].id));
    }

    // --- JS/TS family: tsx/typescript/javascript resolve as one language ---

    /// Insert `symbols` into a fresh file per (path, language) and return
    /// every inserted symbol's id by qualname.
    fn insert_files(
        db: &mut Db,
        files: &[(&str, &str, Vec<SymbolInput>)],
    ) -> (HashMap<String, i64>, HashMap<String, i64>) {
        let mut ids = HashMap::new();
        let mut file_ids = HashMap::new();
        for (i, (path, lang, syms)) in files.iter().enumerate() {
            let file_id = db
                .upsert_file(path, &format!("h{i}"), lang, 100, 0)
                .unwrap();
            file_ids.insert(path.to_string(), file_id);
            for s in db.insert_symbols(file_id, path, syms, 1, None).unwrap() {
                ids.insert(s.qualname.clone(), s.id);
            }
        }
        (ids, file_ids)
    }

    fn only_target(db: &Db, source_id: i64) -> Option<i64> {
        let found = db.edges_for_symbol(source_id, None, 1).unwrap();
        assert_eq!(found.len(), 1);
        found[0].target_symbol_id
    }

    #[test]
    fn test_insert_edges_tsx_receiver_typed_call_binds_method_declared_in_ts() {
        let (mut db, _temp) = create_test_db();
        let (ids, file_ids) = insert_files(
            &mut db,
            &[
                (
                    "lib/svc.ts",
                    "typescript",
                    vec![
                        make_test_symbol("lib/svc.CatalogService", None, "class", 1),
                        make_test_symbol("lib/svc.CatalogService.list", None, "method", 2),
                    ],
                ),
                (
                    "app/page.tsx",
                    "tsx",
                    vec![make_test_symbol("app/page.Page", None, "function", 1)],
                ),
            ],
        );
        let edges = vec![make_test_edge_with_receiver_type(
            "CALLS",
            "app/page.Page",
            "svc.list",
            ReceiverType::Known("CatalogService".to_string()),
        )];
        db.insert_edges(file_ids["app/page.tsx"], &edges, &ids, 1, None)
            .unwrap();
        assert_eq!(
            only_target(&db, ids["app/page.Page"]),
            Some(ids["lib/svc.CatalogService.list"])
        );
    }

    #[test]
    fn test_insert_edges_ts_family_never_binds_python_or_csharp() {
        let (mut db, _temp) = create_test_db();
        let (ids, file_ids) = insert_files(
            &mut db,
            &[
                (
                    "py/mod.py",
                    "python",
                    vec![make_test_symbol("py.mod.pyOnly", None, "function", 1)],
                ),
                (
                    "cs/C.cs",
                    "csharp",
                    vec![make_test_symbol("Ns.C.csOnly", None, "method", 1)],
                ),
                (
                    "app/page.tsx",
                    "tsx",
                    vec![
                        make_test_symbol("app/page.A", None, "function", 1),
                        make_test_symbol("app/page.B", None, "function", 10),
                    ],
                ),
            ],
        );
        let edges = vec![
            make_test_edge("CALLS", "app/page.A", "app/page.pyOnly"),
            make_test_edge("CALLS", "app/page.B", "app/page.csOnly"),
        ];
        db.insert_edges(file_ids["app/page.tsx"], &edges, &ids, 1, None)
            .unwrap();
        db.resolve_null_target_edges(1).unwrap();
        assert_eq!(only_target(&db, ids["app/page.A"]), None);
        assert_eq!(only_target(&db, ids["app/page.B"]), None);
    }

    #[test]
    fn test_insert_edges_ts_import_candidate_miss_refuses_family_fuzzy() {
        let (mut db, _temp) = create_test_db();
        let (ids, file_ids) = insert_files(
            &mut db,
            &[
                (
                    "other/hooks.ts",
                    "typescript",
                    vec![
                        make_test_symbol("other/hooks.useState", None, "function", 1),
                        make_test_symbol("other/hooks.helper", None, "function", 5),
                    ],
                ),
                (
                    "app/page.tsx",
                    "tsx",
                    vec![
                        make_test_symbol("app/page.A", None, "function", 1),
                        make_test_symbol("app/page.B", None, "function", 10),
                    ],
                ),
            ],
        );
        let edges = vec![
            // External package import (`import { useState } from 'react'`).
            make_test_edge_with_import_candidates(
                "CALLS",
                "app/page.A",
                "app/page.useState",
                vec!["react:useState".to_string()],
            ),
            // Repo import whose export isn't declared in the resolved file
            // (a barrel re-export): still must not guess by name.
            make_test_edge_with_import_candidates(
                "CALLS",
                "app/page.B",
                "app/page.helper",
                vec!["lib.helper".to_string()],
            ),
        ];
        db.insert_edges(file_ids["app/page.tsx"], &edges, &ids, 1, None)
            .unwrap();
        db.resolve_null_target_edges(1).unwrap();
        assert_eq!(only_target(&db, ids["app/page.A"]), None);
        assert_eq!(only_target(&db, ids["app/page.B"]), None);
    }

    // --- ambiguity guard: bare-name fuzzy fallback must not bind arbitrarily ---

    #[test]
    fn test_insert_edges_ambiguous_bare_name_stays_null_unambiguous_still_resolves() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("pkg/store.py", "h1", "python", 100, 0)
            .unwrap();

        // Two unrelated same-language "append" methods on different classes, exactly the
        // pathology from the bug report: `list.append` vs. a domain `EventStore.append`.
        // A caller who writes `some_list.append(x)` has no receiver-type information
        // recorded, so the extractor's target_qualname is just an import-relative guess
        // that resolves to neither of these by exact qualname — both are only reachable
        // through the bare-name fuzzy fallback, which is exactly what must now refuse.
        let ambiguous_syms = vec![
            make_test_symbol("builtins.list.append", Some("def append(x)"), "method", 1),
            make_test_symbol(
                "pkg.store.EventStore.append",
                Some("def append(self, event)"),
                "method",
                10,
            ),
            // One unambiguous symbol in the same file/version, to prove the guard
            // doesn't just NULL everything.
            make_test_symbol("pkg.store.compute", Some("def compute()"), "function", 20),
            make_test_symbol("pkg.store.caller", Some("def caller()"), "function", 30),
        ];
        let inserted = db
            .insert_symbols(file_id, "pkg/store.py", &ambiguous_syms, 1, None)
            .unwrap();
        let caller_id = inserted
            .iter()
            .find(|s| s.qualname == "pkg.store.caller")
            .unwrap()
            .id;
        let compute_id = inserted
            .iter()
            .find(|s| s.qualname == "pkg.store.compute")
            .unwrap()
            .id;

        let edges = vec![
            // Bare-name target with two same-language candidates ("...list.append" and
            // "...EventStore.append") -> must stay NULL, not bind to whichever the
            // fuzzy LIKE happens to return first.
            make_test_edge("CALLS", "pkg.store.caller", "append"),
            // Bare-name target with exactly one candidate -> must still resolve.
            make_test_edge("CALLS", "pkg.store.caller", "compute"),
        ];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let found = db.edges_for_symbol(caller_id, None, 1).unwrap();
        assert_eq!(found.len(), 2);

        let append_edge = found
            .iter()
            .find(|e| e.target_qualname.as_deref() == Some("append"))
            .unwrap();
        assert_eq!(
            append_edge.target_symbol_id, None,
            "ambiguous bare-name call must not bind to either same-named candidate"
        );

        let compute_edge = found
            .iter()
            .find(|e| e.target_qualname.as_deref() == Some("compute"))
            .unwrap();
        assert_eq!(
            compute_edge.target_symbol_id,
            Some(compute_id),
            "unambiguous bare-name call must still resolve"
        );
    }

    // --- two-segment tier: `Type::method` disambiguates same-named methods on different types ---

    #[test]
    fn test_insert_edges_two_segment_qualname_disambiguates_same_named_methods() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();

        // Two unrelated Rust types with same-named `new` constructors — the exact
        // pathology from the bug report (`Db::new` colliding with every other
        // `new` in the crate, e.g. `Vec::new`, once resolution is limited to the
        // bare trailing name). Neither call site's target_qualname is a full
        // path, so it can't be found by exact qualname; bare-name-only
        // resolution (tier 2) would see two same-named `new` candidates here and
        // refuse to bind either. The two-segment tier (tier 1) uses the extra
        // `Type::` segment already present in the call site's recorded
        // target_qualname to tell them apart.
        let syms = vec![
            make_test_symbol("crate::db::Db::new", Some("fn new() -> Self"), "method", 1),
            make_test_symbol(
                "crate::cache::Cache::new",
                Some("fn new() -> Self"),
                "method",
                10,
            ),
            make_test_symbol("crate::caller::run", Some("fn run()"), "function", 20),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &syms, 1, None)
            .unwrap();
        let db_new_id = inserted
            .iter()
            .find(|s| s.qualname == "crate::db::Db::new")
            .unwrap()
            .id;
        let cache_new_id = inserted
            .iter()
            .find(|s| s.qualname == "crate::cache::Cache::new")
            .unwrap()
            .id;
        let caller_id = inserted
            .iter()
            .find(|s| s.qualname == "crate::caller::run")
            .unwrap()
            .id;

        let edges = vec![
            make_test_edge("CALLS", "crate::caller::run", "Db::new"),
            make_test_edge("CALLS", "crate::caller::run", "Cache::new"),
        ];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let found = db.edges_for_symbol(caller_id, None, 1).unwrap();
        assert_eq!(found.len(), 2);

        let db_edge = found
            .iter()
            .find(|e| e.target_qualname.as_deref() == Some("Db::new"))
            .unwrap();
        assert_eq!(
            db_edge.target_symbol_id,
            Some(db_new_id),
            "Db::new must resolve to Db's constructor via the two-segment tier"
        );

        let cache_edge = found
            .iter()
            .find(|e| e.target_qualname.as_deref() == Some("Cache::new"))
            .unwrap();
        assert_eq!(
            cache_edge.target_symbol_id,
            Some(cache_new_id),
            "Cache::new must resolve to Cache's constructor, not Db's, even though both share the bare name `new`"
        );
        assert_ne!(
            db_edge.target_symbol_id, cache_edge.target_symbol_id,
            "same-named methods on different types must not collide"
        );
    }

    // --- receiver-type tier: gate resolution on the extractor's inferred receiver type ---

    #[test]
    fn test_insert_edges_receiver_type_known_resolves_via_receiver_type_tier() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/lib.rs", "h1", "python", 100, 0)
            .unwrap();

        // A domain `append` method — the exact collision pathology from
        // issue #45: any call site literally named "<var>.append" would,
        // under the old bare-name tier, be the *only* candidate and bind
        // confidently to this method regardless of the variable's real type.
        let syms = vec![make_test_symbol(
            "pkg.store.EventStore.append",
            Some("def append(self, event)"),
            "method",
            1,
        )];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &syms, 1, None)
            .unwrap();
        let append_id = inserted[0].id;

        // Receiver type inferred from an annotated parameter (`store:
        // EventStore`) — target_qualname is the call site's literal text
        // ("store.append"), NOT rewritten to use the type name; only
        // receiver_type carries the inferred type.
        let edges = vec![make_test_edge_with_receiver_type(
            "CALLS",
            "pkg.caller.run",
            "store.append",
            ReceiverType::Known("EventStore".to_string()),
        )];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let (target_symbol_id, resolution_kind): (Option<i64>, Option<String>) = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, resolution_kind FROM edges WHERE target_qualname = 'store.append'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            target_symbol_id,
            Some(append_id),
            "a known receiver type must resolve to that type's own method"
        );
        assert_eq!(resolution_kind.as_deref(), Some("receiver_type"));
    }

    #[test]
    fn test_insert_edges_receiver_type_unresolved_never_binds() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/lib.rs", "h1", "python", 100, 0)
            .unwrap();

        // Same domain `append` method as above — the only "append" symbol
        // in the index, so the old bare-name tier would bind confidently.
        let syms = vec![make_test_symbol(
            "pkg.store.EventStore.append",
            Some("def append(self, event)"),
            "method",
            1,
        )];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &syms, 1, None)
            .unwrap();

        // Receiver type inferred as a builtin (`cells = []`) — tracked, but
        // must not bind at all, not even speculatively.
        let edges = vec![make_test_edge_with_receiver_type(
            "CALLS",
            "pkg.caller.run",
            "cells.append",
            ReceiverType::Unresolved,
        )];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let (target_symbol_id, receiver_type, resolution_kind): (
            Option<i64>,
            Option<String>,
            Option<String>,
        ) = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, receiver_type, resolution_kind FROM edges WHERE target_qualname = 'cells.append'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            target_symbol_id, None,
            "a builtin/unresolved receiver type must never bind, even though EventStore.append \
             is the sole candidate for the bare name \"append\""
        );
        assert_eq!(
            receiver_type.as_deref(),
            Some(""),
            "the receiver_type column encodes tracked-but-unresolved as an empty string, \
             distinct from NULL (not tracked at all)"
        );
        assert_eq!(resolution_kind, None);
    }

    // --- import tier: import-qualified candidates disambiguate a bare
    // two-segment call whose receiver is a type name, not a tracked local
    // (see `EdgeInput::import_candidates` / `csharp::import_qualified_candidates`) ---

    #[test]
    fn test_insert_edges_import_candidate_resolves_unambiguous_tier() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/Caller.cs", "h1", "csharp", 100, 0)
            .unwrap();

        // Twin-class-shaped setup: a bare `Widget.Create` two-segment call
        // is ambiguous by itself, but only ONE of the candidate namespaces
        // the extractor guessed from this file's `using`s actually names a
        // real symbol.
        let syms = vec![
            make_test_symbol(
                "Dpb.DomainA.Widget.Create",
                Some("static Widget Create()"),
                "method",
                1,
            ),
            make_test_symbol("Dpb.Caller.Run", Some("void Run()"), "method", 20),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/Caller.cs", &syms, 1, None)
            .unwrap();
        let widget_create_id = inserted
            .iter()
            .find(|s| s.qualname == "Dpb.DomainA.Widget.Create")
            .unwrap()
            .id;

        let edges = vec![make_test_edge_with_import_candidates(
            "CALLS",
            "Dpb.Caller.Run",
            "Widget.Create",
            vec![
                "Dpb.DomainA.Widget.Create".to_string(),
                "Dpb.NoSuchNamespace.Widget.Create".to_string(),
            ],
        )];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let (target_symbol_id, resolution_kind): (Option<i64>, Option<String>) = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, resolution_kind FROM edges WHERE target_qualname = 'Widget.Create'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            target_symbol_id,
            Some(widget_create_id),
            "the one import candidate that names a real symbol must bind, \
             even though the literal call-site text (\"Widget.Create\") is \
             ambiguous by itself and the other candidate names nothing"
        );
        assert_eq!(resolution_kind.as_deref(), Some("import"));
    }

    #[test]
    fn test_insert_edges_import_candidate_ambiguous_across_two_real_symbols_refuses() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/Caller.cs", "h1", "csharp", 100, 0)
            .unwrap();

        // The actual twin-class pathology this feature exists to fix:
        // TWO distinct namespaces both really do declare a `Widget` with a
        // `Create` method, so both import candidates resolve to real (but
        // different) symbols. The import tier must refuse rather than pick
        // one -- and so must every tier after it, since the fallback
        // two-segment pattern ("%.Widget.Create") matches both as well.
        let syms = vec![
            make_test_symbol(
                "Dpb.DomainA.Widget.Create",
                Some("static Widget Create()"),
                "method",
                1,
            ),
            make_test_symbol(
                "Dpb.DomainB.Widget.Create",
                Some("static Widget Create()"),
                "method",
                10,
            ),
            make_test_symbol("Dpb.Caller.Run", Some("void Run()"), "method", 20),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/Caller.cs", &syms, 1, None)
            .unwrap();

        let edges = vec![make_test_edge_with_import_candidates(
            "CALLS",
            "Dpb.Caller.Run",
            "Widget.Create",
            vec![
                "Dpb.DomainA.Widget.Create".to_string(),
                "Dpb.DomainB.Widget.Create".to_string(),
            ],
        )];
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let (target_symbol_id, resolution_kind): (Option<i64>, Option<String>) = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, resolution_kind FROM edges WHERE target_qualname = 'Widget.Create'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            target_symbol_id, None,
            "two import candidates that both name real (but different) \
             symbols must not bind to either -- the ambiguity guard must \
             still refuse, exactly as it did before import qualification"
        );
        assert_eq!(resolution_kind, None);
    }

    #[test]
    fn test_insert_edges_import_candidate_unresolved_refuses_bare_name_fallback() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/conftest.py", "h1", "python", 100, 0)
            .unwrap();

        // The actual reported pathology: `datetime.now(timezone.utc)` in a
        // file that does `from datetime import datetime` (stdlib). The
        // extractor resolves "datetime" through this file's own import
        // bindings to a candidate qualname ("datetime.now") that names
        // nothing in this repo's index -- but `now` also happens to be the
        // *only* locally-defined symbol named `now` anywhere in the index
        // (a test double, `FakeClock.now`). Before this fix, a failed
        // import candidate fell through to the old bare-name tier, which
        // saw only "one candidate named `now`" and bound to it -- the
        // stdlib call, resolved to a test fake.
        let syms = vec![make_test_symbol(
            "pkg.tests.conftest.FakeClock.now",
            Some("def now(cls)"),
            "method",
            1,
        )];
        let inserted = db
            .insert_symbols(file_id, "src/conftest.py", &syms, 1, None)
            .unwrap();
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();

        let edges = vec![make_test_edge_with_import_candidates(
            "CALLS",
            "pkg.caller.run",
            "datetime.now",
            vec!["datetime.now".to_string()],
        )];
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let (target_symbol_id, receiver_type, resolution_kind): (
            Option<i64>,
            Option<String>,
            Option<String>,
        ) = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, receiver_type, resolution_kind FROM edges WHERE target_qualname = 'datetime.now'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(
            target_symbol_id, None,
            "a receiver positively known (via this file's own imports) to come from an \
             external module must never fall through to bare-name matching, even though \
             FakeClock.now is the sole local symbol named \"now\""
        );
        assert_eq!(
            receiver_type.as_deref(),
            Some(""),
            "a failed import candidate must be persisted as tracked-but-unresolved, the same \
             column value a receiver-type-tracked builtin uses, so a later repair pass also \
             refuses to fuzzy-resolve it"
        );
        assert_eq!(resolution_kind, None);
    }

    #[test]
    fn test_resolve_null_target_edges_respects_failed_import_candidate() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/conftest.py", "h1", "python", 100, 0)
            .unwrap();

        let syms = vec![make_test_symbol(
            "pkg.tests.conftest.FakeClock.now",
            Some("def now(cls)"),
            "method",
            1,
        )];
        db.insert_symbols(file_id, "src/conftest.py", &syms, 1, None)
            .unwrap();

        // Insert with an empty symbol_map, as during mid-incremental-reindex,
        // so the edge lands with target_symbol_id NULL and only the
        // *persisted* receiver_type is left to guide a later repair pass.
        let edges = vec![make_test_edge_with_import_candidates(
            "CALLS",
            "pkg.caller.run",
            "datetime.now",
            vec!["datetime.now".to_string()],
        )];
        db.insert_edges(file_id, &edges, &HashMap::new(), 1, None)
            .unwrap();

        db.resolve_null_target_edges(1).unwrap();

        let target_symbol_id: Option<i64> = db
            .conn()
            .query_row(
                "SELECT target_symbol_id FROM edges WHERE target_qualname = 'datetime.now'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            target_symbol_id, None,
            "the repair pass has no access to the transient import_candidates list, so it must \
             rely on the persisted receiver_type='' this fix writes -- without it, the repair \
             pass would resurrect the false bare-name match on its own next run"
        );
    }

    /// Insert `syms` into one python file, then one CALLS edge carrying
    /// `candidates`; return the stored (target_symbol_id, receiver_type,
    /// resolution_kind) and the qualname -> id map.
    #[allow(clippy::type_complexity)]
    fn insert_python_import_call(
        syms: &[(&str, &str)],
        target: &str,
        candidates: &[&str],
    ) -> (
        (Option<i64>, Option<String>, Option<String>),
        HashMap<String, i64>,
    ) {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("py/pkg/src/pkg/a.py", "h1", "python", 100, 0)
            .unwrap();
        let syms: Vec<_> = syms
            .iter()
            .enumerate()
            .map(|(i, (qn, kind))| make_test_symbol(qn, None, kind, i as i64 * 10 + 1))
            .collect();
        let inserted = db
            .insert_symbols(file_id, "py/pkg/src/pkg/a.py", &syms, 1, None)
            .unwrap();
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        let edges = vec![make_test_edge_with_import_candidates(
            "CALLS",
            "py.pkg.src.pkg.a.caller",
            target,
            candidates.iter().map(|c| c.to_string()).collect(),
        )];
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();
        let row = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, receiver_type, resolution_kind FROM edges WHERE kind = 'CALLS'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        (row, symbol_map)
    }

    #[test]
    fn test_insert_edges_bare_external_import_shadows_unique_repo_method() {
        // `from urllib.parse import quote; quote(x)` in a repo whose only
        // `quote` is an unrelated method. The external import must shadow
        // the name: no bare-name binding to `MssqlCodeWriter.quote`.
        let ((target, receiver_type, _), _) = insert_python_import_call(
            &[
                ("py.pkg.src.pkg", "module"),
                ("py.pkg.src.pkg.writer.MssqlCodeWriter.quote", "method"),
            ],
            "py.pkg.src.pkg.a.quote",
            &["urllib.parse.quote"],
        );
        assert_eq!(target, None, "external import must not bind a repo symbol");
        assert_eq!(receiver_type.as_deref(), Some(""));
    }

    #[test]
    fn test_insert_edges_import_candidate_matches_src_layout_qualname_by_suffix() {
        // dpb shape: module qualnames carry the repo path prefix
        // (`py.pkg.src.`) the import statement does not.
        let ((target, _, kind), map) = insert_python_import_call(
            &[
                ("py.pkg.src.pkg", "module"),
                ("py.pkg.src.pkg.runtime.run", "function"),
                ("py.other.src.other.run", "function"),
            ],
            "py.pkg.src.pkg.a.run",
            &["pkg.runtime.run"],
        );
        assert_eq!(target, Some(map["py.pkg.src.pkg.runtime.run"]));
        assert_eq!(kind.as_deref(), Some("import"));
    }

    #[test]
    fn test_insert_edges_unresolved_repo_import_keeps_fuzzy_fallback() {
        // `from pkg import helper` where `pkg/__init__.py` re-exports
        // `helper` from `pkg.core`: the candidate names nothing, but its
        // root is a repo package, so the pre-existing bare-name tier still
        // runs instead of the edge being refused as external.
        let ((target, receiver_type, kind), map) = insert_python_import_call(
            &[
                ("py.pkg.src.pkg", "module"),
                ("py.pkg.src.pkg.core.helper", "function"),
            ],
            "py.pkg.src.pkg.a.helper",
            &["pkg.helper"],
        );
        assert_eq!(target, Some(map["py.pkg.src.pkg.core.helper"]));
        assert_eq!(kind.as_deref(), Some("bare_name"));
        assert_eq!(receiver_type, None);
    }

    #[test]
    fn test_insert_edges_generated_pb2_import_under_repo_package_is_external() {
        // `from pkg.v1 import pkg_pb2 as pb; pb.ColumnDef(...)`: the pb2
        // module is protoc output, so the repo dataclass of the same name
        // must not be picked up by the fuzzy tiers.
        let ((target, _, _), _) = insert_python_import_call(
            &[
                ("py.pkg.src.pkg", "module"),
                ("py.pkg.src.pkg.schema.ColumnDef", "class"),
            ],
            "pb.ColumnDef",
            &["pkg.v1.pkg_pb2.ColumnDef"],
        );
        assert_eq!(target, None);
    }

    #[test]
    fn test_resolve_null_target_edges_import_suffix_round_repairs_edge() {
        // Incremental-reindex shape: the edge lands before its target's
        // symbol exists, then the repair pass must find it via the suffix
        // round (exact never matches a src-layout qualname).
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("py/pkg/src/pkg/a.py", "h1", "python", 100, 0)
            .unwrap();
        let edges = vec![make_test_edge_with_import_candidates(
            "CALLS",
            "py.pkg.src.pkg.a.caller",
            "py.pkg.src.pkg.a.run",
            vec!["pkg.runtime.run".to_string()],
        )];
        db.insert_edges(file_id, &edges, &HashMap::new(), 1, None)
            .unwrap();
        let other = db
            .upsert_file("py/pkg/src/pkg/runtime.py", "h2", "python", 100, 0)
            .unwrap();
        let inserted = db
            .insert_symbols(
                other,
                "py/pkg/src/pkg/runtime.py",
                &[make_test_symbol(
                    "py.pkg.src.pkg.runtime.run",
                    None,
                    "function",
                    1,
                )],
                1,
                None,
            )
            .unwrap();
        db.resolve_null_target_edges(1).unwrap();
        let (target, kind): (Option<i64>, Option<String>) = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, resolution_kind FROM edges WHERE kind = 'CALLS'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(target, Some(inserted[0].id));
        assert_eq!(kind.as_deref(), Some("import"));
    }

    #[test]
    fn test_resolve_null_target_edges_respects_persisted_receiver_type() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/lib.rs", "h1", "python", 100, 0)
            .unwrap();

        let syms = vec![make_test_symbol(
            "pkg.store.EventStore.append",
            Some("def append(self, event)"),
            "method",
            1,
        )];
        db.insert_symbols(file_id, "src/lib.rs", &syms, 1, None)
            .unwrap();

        // Insert with a symbol_map that deliberately can't resolve anything
        // (simulating the edge landing with target_symbol_id NULL, as it
        // would mid-incremental-reindex), then run the repair pass.
        let edges = vec![make_test_edge_with_receiver_type(
            "CALLS",
            "pkg.caller.run",
            "cells.append",
            ReceiverType::Unresolved,
        )];
        db.insert_edges(file_id, &edges, &HashMap::new(), 1, None)
            .unwrap();

        db.resolve_null_target_edges(1).unwrap();

        let target_symbol_id: Option<i64> = db
            .conn()
            .query_row(
                "SELECT target_symbol_id FROM edges WHERE target_qualname = 'cells.append'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            target_symbol_id, None,
            "a repair pass must respect the persisted receiver_type signal just like the \
             original insert — it must not fuzzy-resolve an edge marked unresolved"
        );
    }

    #[test]
    fn test_insert_edges_receiver_type_inherited_method_resolves_via_ancestor() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/lib.rs", "h1", "python", 100, 0)
            .unwrap();

        // MssqlCodeWriter inherits write_line from CodeWriter without
        // overriding it — the dpb gap this tier closes. Only CodeWriter
        // declares the method; MssqlCodeWriter has no symbol of its own
        // named write_line.
        let syms = vec![
            make_test_symbol(
                "pkg.CodeWriter.write_line",
                Some("def write_line(self, s)"),
                "method",
                1,
            ),
            make_test_symbol("pkg.MssqlCodeWriter", None, "class", 20),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &syms, 1, None)
            .unwrap();
        let write_line_id = inserted
            .iter()
            .find(|s| s.qualname == "pkg.CodeWriter.write_line")
            .unwrap()
            .id;
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();

        // `class MssqlCodeWriter(CodeWriter):` — recorded as an EXTENDS edge,
        // same as the real Python extractor emits — plus the call site
        // itself, gated by the inferred receiver type.
        let edges = vec![
            make_test_edge_with_receiver_type(
                "EXTENDS",
                "pkg.MssqlCodeWriter",
                "CodeWriter",
                ReceiverType::NotTracked,
            ),
            make_test_edge_with_receiver_type(
                "CALLS",
                "pkg.caller.run",
                "cw.write_line",
                ReceiverType::Known("MssqlCodeWriter".to_string()),
            ),
        ];
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let (target_symbol_id, resolution_kind): (Option<i64>, Option<String>) = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, resolution_kind FROM edges WHERE target_qualname = 'cw.write_line'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            target_symbol_id,
            Some(write_line_id),
            "a call through a subclass-typed receiver must bind to the base class's method \
             when the subclass itself declares no override"
        );
        assert_eq!(
            resolution_kind.as_deref(),
            Some("inherited"),
            "an inherited bind must be tagged distinctly from a direct receiver_type match"
        );
    }

    #[test]
    fn test_insert_edges_receiver_type_inherited_method_ambiguous_bases_refuses() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/lib.rs", "h1", "python", 100, 0)
            .unwrap();

        // `class Foo(A, B):` where *both* A and B declare `method` — Python
        // multiple inheritance with no way to tell, from the recorded
        // hierarchy alone, which base the language would actually dispatch
        // to. Must refuse rather than guess.
        let syms = vec![
            make_test_symbol("pkg.A.method", Some("def method(self)"), "method", 1),
            make_test_symbol("pkg.B.method", Some("def method(self)"), "method", 10),
            make_test_symbol("pkg.Foo", None, "class", 20),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &syms, 1, None)
            .unwrap();
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();

        let edges = vec![
            make_test_edge_with_receiver_type("EXTENDS", "pkg.Foo", "A", ReceiverType::NotTracked),
            make_test_edge_with_receiver_type("EXTENDS", "pkg.Foo", "B", ReceiverType::NotTracked),
            make_test_edge_with_receiver_type(
                "CALLS",
                "pkg.caller.run",
                "foo.method",
                ReceiverType::Known("Foo".to_string()),
            ),
        ];
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let (target_symbol_id, resolution_kind): (Option<i64>, Option<String>) = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, resolution_kind FROM edges WHERE target_qualname = 'foo.method'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            target_symbol_id, None,
            "two unrelated ancestors declaring the same method is ambiguous and must not bind"
        );
        assert_eq!(resolution_kind, None);
    }

    #[test]
    fn test_insert_edges_receiver_type_direct_override_wins_over_inherited() {
        let (mut db, _temp) = create_test_db();
        let file_id = db
            .upsert_file("src/lib.rs", "h1", "python", 100, 0)
            .unwrap();

        // MssqlCodeWriter *does* override write_line this time — the direct
        // receiver_type tier must still win, and the ancestor's own
        // write_line (also present) must not be walked to or preferred.
        let syms = vec![
            make_test_symbol(
                "pkg.MssqlCodeWriter.write_line",
                Some("def write_line(self, s)"),
                "method",
                1,
            ),
            make_test_symbol(
                "pkg.CodeWriter.write_line",
                Some("def write_line(self, s)"),
                "method",
                10,
            ),
            make_test_symbol("pkg.MssqlCodeWriter", None, "class", 20),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &syms, 1, None)
            .unwrap();
        let own_write_line_id = inserted
            .iter()
            .find(|s| s.qualname == "pkg.MssqlCodeWriter.write_line")
            .unwrap()
            .id;
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();

        let edges = vec![
            make_test_edge_with_receiver_type(
                "EXTENDS",
                "pkg.MssqlCodeWriter",
                "CodeWriter",
                ReceiverType::NotTracked,
            ),
            make_test_edge_with_receiver_type(
                "CALLS",
                "pkg.caller.run",
                "cw.write_line",
                ReceiverType::Known("MssqlCodeWriter".to_string()),
            ),
        ];
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();

        let (target_symbol_id, resolution_kind): (Option<i64>, Option<String>) = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, resolution_kind FROM edges WHERE target_qualname = 'cw.write_line'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            target_symbol_id,
            Some(own_write_line_id),
            "a direct (non-inherited) call must still bind to the receiver's own method, \
             not walk past it to an ancestor that happens to declare the same name"
        );
        assert_eq!(
            resolution_kind.as_deref(),
            Some("receiver_type"),
            "a direct match must keep the existing receiver_type resolution_kind, not \
             \"inherited\" — the walk must never even run when the direct tier already hit"
        );
    }

    #[test]
    fn test_lookup_symbol_id_fuzzy_shortest_wins_across_separator_styles() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("src/lib.rs", "h1", "rust", 100, 0).unwrap();

        // One dot-style and one colons-style candidate; the colons one is shorter
        let symbols = vec![
            make_test_symbol("a.b.deeply.process", Some("fn process()"), "function", 1),
            make_test_symbol("x::process", Some("fn process()"), "function", 10),
        ];
        let inserted = db
            .insert_symbols(file_id, "src/lib.rs", &symbols, 1, None)
            .unwrap();

        // Shortest qualname wins across both LIKE branches
        let id = db.lookup_symbol_id_fuzzy("process", None, 1).unwrap();
        assert_eq!(id, Some(inserted[1].id));
    }
}
