use crate::config::Config;
use crate::indexer::differ::SymbolDiff;
use crate::indexer::extract::{DeferredMarker, EdgeInput, ReceiverType, SymbolInput};
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
pub use graph_query::{DispatchPeers, EntryArgs, closed_impl_args, dispatch_compatible, type_args};
mod migrations;
mod overview;
pub(crate) mod resolver;

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
    pub path: String,
    pub visibility: Option<String>,
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

/// `graph_versions.status` of a version a reindex is still populating.
const GV_BUILDING: &str = "building";
/// `graph_versions.status` of a fully populated version.
const GV_COMPLETE: &str = "complete";

/// Proof the holder may reindex this database; released on drop (including
/// on error paths and panics). See [`Db::try_lock_reindex`].
#[must_use = "the reindex lock is released when this guard is dropped"]
pub struct ReindexLock {
    _file: std::fs::File,
}

impl Drop for ReindexLock {
    fn drop(&mut self) {
        // Unlock explicitly: a concurrent `fork` (e.g. spawning `git`) can
        // briefly hold a duplicate of this descriptor, and closing ours alone
        // would not release the lock until that child execs.
        let _ = self._file.unlock();
    }
}

/// Another process holds the reindex lock (issue #250).
#[derive(Debug)]
pub struct ReindexBusy {
    pub holder_pid: Option<u32>,
    pub lock_path: PathBuf,
}

impl std::fmt::Display for ReindexBusy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "another reindex is already running against this database"
        )?;
        if let Some(pid) = self.holder_pid {
            write!(f, " (pid {pid})")?;
        }
        write!(
            f,
            "; nothing was modified. Retry when it finishes (lock: {})",
            self.lock_path.display()
        )
    }
}

impl std::error::Error for ReindexBusy {}

pub struct Db {
    db_path: PathBuf,
    write_conn: Arc<Mutex<Connection>>,
    read_pool: Pool<SqliteConnectionManager>,
}

/// Number of most-recent graph versions whose `symbols`/`edges` rows survive
/// `prune_old_graph_versions`. `carry_forward_symbols`/`carry_forward_references` (reindex's unchanged-file
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

/// Token returned by `Db::carry_forward_symbols`: proof the symbol phase of
/// carry-forward ran, and the arguments `Db::carry_forward_references` needs
/// for the second phase. Only `carry_forward_symbols` can build one, so the
/// phases can't run out of order (issue #258).
#[must_use = "pass to Db::carry_forward_references after the fresh-file edge writes"]
pub struct SymbolsCarried<'a> {
    file_ids: &'a [i64],
    from_version: i64,
    to_version: i64,
    /// Symbols copied into `to_version`.
    pub symbols: usize,
}

impl SymbolsCarried<'_> {
    fn placeholders(&self) -> String {
        vec!["?"; self.file_ids.len()].join(",")
    }

    /// Number of unchanged files being carried forward.
    pub fn file_count(&self) -> usize {
        self.file_ids.len()
    }
}

/// What `Db::carry_forward_references` copied.
#[derive(Debug, Default, Clone, Copy)]
pub struct ReferencesCarried {
    /// External stub symbols copied.
    pub stubs: usize,
    /// Edges copied.
    pub edges: usize,
}

/// One `carry_forward_references` `unresolved_references` row awaiting remap to
/// `to_version`'s edge/symbol ids -- a named struct rather than a tuple,
/// since it's wide enough to trip `clippy::type_complexity` (same reasoning
/// as `resolver::StoreRetryRow`/`NullTargetEdgeRow`).
struct CarriedUnresolvedRow {
    old_edge_id: i64,
    old_source_symbol_id: Option<i64>,
    file_id: i64,
    edge_kind: String,
    reference_name: Option<String>,
    name_tail: String,
    reason: String,
    import_candidates: Option<String>,
    detail: Option<String>,
    evidence_snippet: Option<String>,
    evidence_start_line: Option<i64>,
    evidence_end_line: Option<i64>,
    confidence: Option<f64>,
    commit_sha: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    event_ts: Option<i64>,
    receiver_type: Option<String>,
    bare_call: bool,
    call_shape: Option<String>,
    receiver_scope: Option<String>,
    deferred_kind: Option<String>,
    deferred: Option<String>,
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

    /// Every real, currently-live file -- both of this method's callers
    /// (`Indexer::changed_files`, `Indexer::reindex`) compare it against a
    /// filesystem scan to decide what's added/modified/deleted, so issue
    /// #80's single synthetic external pseudo-file (`Resolver::
    /// external_file_id`, `files.language = 'external'`) is excluded here:
    /// it never appears in a filesystem scan (it isn't a real file), so
    /// either caller would otherwise treat it as permanently "deleted" on
    /// every single reindex -- and `Db::mark_file_deleted` stamping its
    /// `deleted_version` with the *current* graph_version would make every
    /// stub symbol on it invisible to any query that also checks its own
    /// file's `deleted_version` (`Resolver::same_lang_lookup`'s candidate
    /// query, `dead_symbols`, ...) starting in that very version.
    pub fn list_files(&self, graph_version: i64) -> Result<Vec<FileRecord>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, path, hash, language, deleted_version
             FROM files
             WHERE (deleted_version IS NULL OR deleted_version > ?)
               AND language != 'external'
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

    /// Delete every edge of `kind` in `graph_version`, plus that kind's
    /// unresolved-reference store rows (pending rows have no edge to cascade
    /// from, so a re-derivation would otherwise duplicate them -- issue #251).
    pub fn delete_edges_and_references_by_kind(
        &self,
        kind: &str,
        graph_version: i64,
    ) -> Result<()> {
        self.conn().execute(
            "DELETE FROM edges WHERE kind = ? AND graph_version = ?",
            params![kind, graph_version],
        )?;
        // Pending store rows (`edge_id` NULL, e.g. every ROUTE reference)
        // have no edge to cascade from; without this a re-derivation of the
        // kind adds a second copy beside the carried-forward one (issue #251).
        self.conn().execute(
            "DELETE FROM unresolved_references WHERE edge_kind = ? AND graph_version = ?",
            params![kind, graph_version],
        )?;
        Ok(())
    }

    /// Delete edges for a file (helper for incremental updates), plus the
    /// file's unresolved-reference store rows: a pending row (`edge_id` NULL)
    /// has no edge to cascade from, so it would otherwise outlive a re-sync.
    pub fn delete_edges_for_file(&self, file_id: i64, graph_version: i64) -> Result<()> {
        self.conn().execute(
            "DELETE FROM edges WHERE file_id = ? AND graph_version = ?",
            params![file_id, graph_version],
        )?;
        self.conn().execute(
            "DELETE FROM unresolved_references WHERE file_id = ? AND graph_version = ?",
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
        self.delete_edges_for_file(file_id, graph_version)?;
        reown_shared_namespace_edges(&self.conn(), file_id, graph_version, None)?;
        // Deleting these symbols nulls any other file's edge that still
        // references one of them via `edges`' `ON DELETE SET NULL` foreign
        // key (issue #76) -- no manual nulling needed here.
        self.conn().execute(
            "DELETE FROM symbols WHERE file_id = ? AND graph_version = ?",
            params![file_id, graph_version],
        )?;
        self.conn().execute(
            "DELETE FROM file_metrics WHERE file_id = ?",
            params![file_id],
        )?;
        self.delete_py_decls(file_id, graph_version)?;
        Ok(())
    }

    /// Store `decls` as `file_id`'s Python declarations at `graph_version`.
    /// Returns whether they differ from the file's previous payload: the row
    /// at this version (an incremental sync rewrites it) or else the latest
    /// older version's (a reindex). A file with no earlier row counts as
    /// changed.
    pub fn put_py_decls(
        &self,
        file_id: i64,
        graph_version: i64,
        decls: &crate::indexer::python_types::PyFileDecls,
    ) -> Result<bool> {
        let payload = decls.to_payload();
        let hash = crate::indexer::python_types::payload_hash(&payload);
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let previous: Option<String> = tx
            .query_row(
                "SELECT hash FROM py_decls WHERE file_id = ? AND graph_version <= ?
                 ORDER BY graph_version DESC LIMIT 1",
                params![file_id, graph_version],
                |row| row.get(0),
            )
            .optional()?;
        tx.execute(
            "INSERT INTO py_decls (file_id, graph_version, hash, payload) VALUES (?, ?, ?, ?)
             ON CONFLICT(graph_version, file_id)
             DO UPDATE SET hash = excluded.hash, payload = excluded.payload",
            params![file_id, graph_version, hash, payload],
        )?;
        tx.commit()?;
        Ok(previous.as_deref() != Some(hash.as_str()))
    }

    /// Remove a file's Python declarations at `graph_version`. Returns
    /// whether a row existed.
    pub fn delete_py_decls(&self, file_id: i64, graph_version: i64) -> Result<bool> {
        let deleted = self.conn().execute(
            "DELETE FROM py_decls WHERE file_id = ? AND graph_version = ?",
            params![file_id, graph_version],
        )?;
        Ok(deleted > 0)
    }

    /// The declarations of every live Python file at `graph_version`, sorted
    /// by path. A payload that no longer parses is skipped with a warning.
    pub fn py_decls(
        &self,
        conn: &Connection,
        graph_version: i64,
    ) -> Result<Vec<crate::indexer::python_types::PyLoadedFile>> {
        use crate::indexer::python_types::{PyFileDecls, PyLoadedFile};
        let mut stmt = conn.prepare(
            "SELECT c.file_id, f.path, c.payload
             FROM py_decls c JOIN files f ON f.id = c.file_id
             WHERE c.graph_version = ?1
               AND (f.deleted_version IS NULL OR f.deleted_version > ?1)
             ORDER BY f.path",
        )?;
        let rows = stmt.query_map(params![graph_version], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut files = Vec::new();
        for row in rows {
            let (file_id, path, payload) = row?;
            match PyFileDecls::from_payload(&payload) {
                Ok(decls) => files.push(PyLoadedFile {
                    file_id,
                    path,
                    decls,
                }),
                Err(err) => eprintln!("lidx: unreadable py_decls payload for {path}: {err}"),
            }
        }
        Ok(files)
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
    /// Split in two (issue #258), phase order enforced by the type system:
    /// `carry_forward_symbols` must run before any edge in the reindex is
    /// resolved, so resolution sees every symbol of `to_version` (candidate
    /// sets and ambiguity verdicts then match a fresh index). It returns a
    /// `SymbolsCarried` token, which `carry_forward_references` consumes, so
    /// the second phase can't run first. Callers run `carry_forward_references`
    /// only after every fresh-file edge write.
    pub fn carry_forward_symbols<'a>(
        &self,
        file_ids: &'a [i64],
        from_version: i64,
        to_version: i64,
    ) -> Result<SymbolsCarried<'a>> {
        let carried = SymbolsCarried {
            file_ids,
            from_version,
            to_version,
            symbols: 0,
        };
        if carried.file_ids.is_empty() {
            return Ok(carried);
        }
        let conn = self.conn();
        let placeholders = carried.placeholders();
        let sql = format!(
            "INSERT INTO symbols
                (file_id, kind, name, qualname, start_line, start_col, end_line, end_col,
                 start_byte, end_byte, signature, docstring, graph_version, commit_sha, stable_id, visibility)
             SELECT file_id, kind, name, qualname, start_line, start_col, end_line, end_col,
                    start_byte, end_byte, signature, docstring, ?, commit_sha, stable_id, visibility
             FROM symbols
             WHERE graph_version = ? AND file_id IN ({placeholders})"
        );
        let mut params: Vec<Box<dyn rusqlite::ToSql>> =
            vec![Box::new(to_version), Box::new(from_version)];
        for id in file_ids {
            params.push(Box::new(*id));
        }
        let symbols = conn.execute(
            &sql,
            rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
        )?;
        // Python declarations ride along, before any edge is resolved.
        let decls_sql = format!(
            "INSERT INTO py_decls (file_id, graph_version, hash, payload)
             SELECT file_id, ?, hash, payload FROM py_decls
             WHERE graph_version = ? AND file_id IN ({placeholders})
             ON CONFLICT DO NOTHING"
        );
        conn.execute(
            &decls_sql,
            rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
        )?;
        Ok(SymbolsCarried { symbols, ..carried })
    }

    /// Second phase of carry-forward: external stubs, edges (with Bridge Edge
    /// re-linking), `symbol_metrics` and stored unresolved references,
    /// remapped by `stable_id` onto the symbols `carry_forward_symbols`
    /// already copied. Run only after every fresh-file edge write, so carried
    /// edges are copied after them (a carried edge whose target lives in a
    /// re-parsed file needs that file's new symbol row).
    pub fn carry_forward_references(
        &self,
        carried: SymbolsCarried<'_>,
    ) -> Result<ReferencesCarried> {
        let SymbolsCarried {
            file_ids,
            from_version,
            to_version,
            ..
        } = carried;
        if file_ids.is_empty() {
            return Ok(ReferencesCarried::default());
        }

        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let placeholders = carried.placeholders();

        // Issue #80: external stub symbols (`kind = 'external'`) aren't
        // owned by any of `file_ids` -- they all live on the one synthetic
        // external pseudo-file (`Resolver::external_file_id`), never a
        // scanned repo file -- so `carry_forward_symbols`' per-file copy never
        // carries them forward. But an edge from one of these carried files
        // into a stub, copied by `edges_sql` below, still needs that stub to
        // already exist in `to_version` for its stable_id-based remap to
        // find. Carry every stub in `from_version` forward unconditionally
        // (not just ones these particular files call): cheap (stubs are
        // few), and simpler than computing which ones this batch's carried
        // edges actually still reference. `ON CONFLICT DO NOTHING` against
        // the `(graph_version, qualname)` partial unique index (schema
        // v20) makes this a no-op wherever `Indexer::reindex`'s fresh-file
        // edge loop -- which runs before this function -- already created
        // the same qualname's stub. A stub with no surviving caller after
        // this reindex is swept by `Db::prune_orphan_external_symbols` in
        // the repair pass, same as a fresh reindex would simply never have
        // created it.
        let stubs_copied = tx.execute(
            "INSERT INTO symbols
                (file_id, kind, name, qualname, start_line, start_col, end_line, end_col,
                 start_byte, end_byte, signature, docstring, graph_version, commit_sha, stable_id, visibility)
             SELECT file_id, kind, name, qualname, start_line, start_col, end_line, end_col,
                    start_byte, end_byte, signature, docstring, ?, commit_sha, stable_id, visibility
             FROM symbols
             WHERE graph_version = ? AND kind = 'external'
             ON CONFLICT DO NOTHING",
            params![to_version, from_version],
        )?;

        // Issue #79: whether any of these carried files have a *Bridge Edge
        // kind* stored unresolved reference to carry forward -- the only
        // shape left needing the edge-id remap below, since a pending
        // (non-Bridge-Edge-kind) reference has no edge to remap at all (see
        // the plain copy further down). The common carry-forward has none,
        // so this stays the cheap, unchanged `execute` path below; only
        // when it's true do we pay for the ordered `RETURNING`-based
        // edge-id remap the copy needs.
        let has_bridge_unresolved: bool = {
            let sql = format!(
                "SELECT EXISTS(SELECT 1 FROM unresolved_references
                 WHERE graph_version = ? AND edge_id IS NOT NULL AND file_id IN ({placeholders}))"
            );
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(from_version)];
            for id in file_ids {
                params.push(Box::new(*id));
            }
            tx.query_row(
                &sql,
                rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
                |row| row.get(0),
            )?
        };

        // `stable_id` has no file component, so several files can share one
        // (shared namespace, identical helper, partial class). A source (and a
        // metrics/store row) remaps within its own file; a target prefers its
        // own file's copy, else the earliest path -- fresh indexing's rule.
        //
        // ponytail: an edge endpoint with no stable_id match in `to_version`
        // (deleted target, or a stable_id collision) is copied with that endpoint
        // NULL rather than dropped — the same best-effort contract the rest of the
        // edge-resolution code (insert_edges' fuzzy fallback, the store-driven
        // repair pass in db::resolver) already has for unresolved targets.
        let edges_sql = format!(
            "INSERT INTO edges
                (file_id, source_symbol_id, target_symbol_id, kind, target_qualname, detail,
                 evidence_snippet, evidence_start_line, evidence_end_line, confidence,
                 graph_version, commit_sha, trace_id, span_id, event_ts,
                 receiver_type, resolution_kind, import_candidates, bare_call, call_shape,
                 receiver_scope, deferred_kind, deferred, derived)
             SELECT
                e.file_id,
                (SELECT ns.id FROM symbols ns
                    WHERE ns.stable_id = src.stable_id AND ns.graph_version = ?
                      AND ns.file_id = src.file_id
                    ORDER BY ns.id LIMIT 1),
                COALESCE(
                    (SELECT nt.id FROM symbols nt
                        WHERE nt.stable_id = tgt.stable_id AND nt.graph_version = ?
                          AND nt.file_id = tgt.file_id
                        ORDER BY nt.id LIMIT 1),
                    (SELECT nt.id FROM symbols nt JOIN files nf ON nf.id = nt.file_id
                        WHERE nt.stable_id = tgt.stable_id AND nt.graph_version = ?
                        ORDER BY nf.path, nt.id LIMIT 1)),
                e.kind, e.target_qualname, e.detail, e.evidence_snippet,
                e.evidence_start_line, e.evidence_end_line, e.confidence,
                ?, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                e.receiver_type, e.resolution_kind, e.import_candidates, e.bare_call, e.call_shape,
                e.receiver_scope, e.deferred_kind, e.deferred, e.derived
             FROM edges e
             LEFT JOIN symbols src ON src.id = e.source_symbol_id
             LEFT JOIN symbols tgt ON tgt.id = e.target_symbol_id
             WHERE e.graph_version = ? AND e.file_id IN ({placeholders})"
        );

        // Old->new edge id map, populated only on the `has_unresolved` path
        // below (an edge has no other cross-version identity to key a
        // remap on, unlike a symbol's `stable_id`) -- empty otherwise.
        let mut edge_id_map: HashMap<i64, i64> = HashMap::new();

        let edges_copied = if !has_bridge_unresolved {
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![
                Box::new(to_version),
                Box::new(to_version),
                Box::new(to_version),
                Box::new(to_version),
                Box::new(from_version),
            ];
            for id in file_ids {
                params.push(Box::new(*id));
            }
            tx.execute(
                &edges_sql,
                rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
            )?
        } else {
            // This batch has Bridge Edge kind store rows to carry (see
            // `has_bridge_unresolved` above). `edges.id` is a plain
            // autoincrement rowid with no
            // stable, cross-version identity of its own (unlike a symbol's
            // `stable_id`), so the only way to learn which new row a given
            // old row became is to sort both queries identically
            // (`ORDER BY e.id ASC`) and pair them up position-by-position:
            // the ordered `INSERT ... SELECT` assigns new rowids in ascending
            // select order, so the Nth smallest new id is the copy of the Nth
            // id in `old_edge_ids` below. `RETURNING` itself emits rows in an
            // arbitrary order (SQLite docs), hence the sort. Both queries run
            // back to back in this same transaction with no intervening write
            // to `edges`, so nothing can change the set between them.
            let old_edge_ids: Vec<i64> = {
                let sql = format!(
                    "SELECT e.id FROM edges e
                     WHERE e.graph_version = ? AND e.file_id IN ({placeholders})
                     ORDER BY e.id ASC"
                );
                let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(from_version)];
                for id in file_ids {
                    params.push(Box::new(*id));
                }
                let mut stmt = tx.prepare(&sql)?;
                let rows = stmt.query_map(
                    rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
                    |row| row.get(0),
                )?;
                rows.collect::<rusqlite::Result<Vec<i64>>>()?
            };

            let mut new_edge_ids: Vec<i64> = {
                let ordered_sql = format!("{edges_sql} ORDER BY e.id ASC RETURNING id");
                let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![
                    Box::new(to_version),
                    Box::new(to_version),
                    Box::new(to_version),
                    Box::new(to_version),
                    Box::new(from_version),
                ];
                for id in file_ids {
                    params.push(Box::new(*id));
                }
                let mut stmt = tx.prepare(&ordered_sql)?;
                let rows = stmt.query_map(
                    rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
                    |row| row.get(0),
                )?;
                rows.collect::<rusqlite::Result<Vec<i64>>>()?
            };

            new_edge_ids.sort_unstable();
            let edges_copied = new_edge_ids.len();
            edge_id_map = old_edge_ids.into_iter().zip(new_edge_ids).collect();
            edges_copied
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
                        WHERE ns.stable_id = os.stable_id AND ns.graph_version = ?
                          AND ns.file_id = os.file_id ORDER BY ns.id LIMIT 1),
                    sm.file_id, sm.loc, sm.complexity, sm.duplication_hash
                 FROM symbol_metrics sm
                 JOIN symbols os ON os.id = sm.symbol_id
                 WHERE os.graph_version = ? AND os.file_id IN ({placeholders})
                   AND (SELECT ns.id FROM symbols ns
                        WHERE ns.stable_id = os.stable_id AND ns.graph_version = ?
                          AND ns.file_id = os.file_id ORDER BY ns.id LIMIT 1) IS NOT NULL"
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

        // Issue #79: carry each carried file's stored unresolved reference
        // forward too, remapped to `to_version`. The store no longer keys
        // on a live edge (self-contained now), so this splits into two
        // independent shapes:
        //
        // - a pending (non-Bridge-Edge-kind) row has no edge at all -- a
        //   plain row copy, remapped by `stable_id` exactly like
        //   `symbol_metrics` above, no edge-id bookkeeping needed.
        // - a Bridge Edge kind row's edge was just copied by the bulk
        //   `INSERT` above -- it still needs `edge_id_map`'s remap to that
        //   copy's *new* edge id.
        //
        // Without either, a carried-forward reference that was already
        // unresolved would arrive in `to_version` with no store row at all,
        // so `reconcile_unresolved_reference_store` would treat it as
        // never-seen and re-resolve it from scratch on every subsequent
        // repair pass -- for a large carried-forward set, that's exactly
        // the untargeted rescan issue #78 removed from the repair sites in
        // the first place, just relocated.
        {
            let sql = format!(
                "INSERT INTO unresolved_references
                    (source_symbol_id, file_id, edge_kind, reference_name, name_tail, reason,
                     import_candidates, detail, evidence_snippet, evidence_start_line,
                     evidence_end_line, confidence, commit_sha, trace_id, span_id, event_ts,
                     receiver_type, bare_call, call_shape, graph_version, receiver_scope,
                     deferred_kind, deferred)
                 SELECT
                    (SELECT ns.id FROM symbols ns
                        WHERE ns.stable_id = os.stable_id AND ns.graph_version = ?
                          AND ns.file_id = os.file_id ORDER BY ns.id LIMIT 1),
                    ur.file_id, ur.edge_kind, ur.reference_name, ur.name_tail, ur.reason,
                    ur.import_candidates, ur.detail, ur.evidence_snippet, ur.evidence_start_line,
                    ur.evidence_end_line, ur.confidence, ur.commit_sha, ur.trace_id, ur.span_id,
                    ur.event_ts, ur.receiver_type, ur.bare_call, ur.call_shape, ?,
                    ur.receiver_scope, ur.deferred_kind, ur.deferred
                 FROM unresolved_references ur
                 LEFT JOIN symbols os ON os.id = ur.source_symbol_id
                 WHERE ur.edge_id IS NULL AND ur.graph_version = ? AND ur.file_id IN ({placeholders})
                 ON CONFLICT DO NOTHING"
            );
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![
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
            )?;
        }

        if !edge_id_map.is_empty() {
            let sql = format!(
                "SELECT ur.edge_id, ur.source_symbol_id, ur.file_id, ur.edge_kind,
                        ur.reference_name, ur.name_tail, ur.reason, ur.import_candidates,
                        ur.detail, ur.evidence_snippet, ur.evidence_start_line,
                        ur.evidence_end_line, ur.confidence, ur.commit_sha, ur.trace_id,
                        ur.span_id, ur.event_ts, ur.receiver_type, ur.bare_call, ur.call_shape,
                        ur.receiver_scope, ur.deferred_kind, ur.deferred
                 FROM unresolved_references ur
                 WHERE ur.edge_id IS NOT NULL AND ur.graph_version = ?
                   AND ur.file_id IN ({placeholders})"
            );
            let mut params: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(from_version)];
            for id in file_ids {
                params.push(Box::new(*id));
            }
            let rows: Vec<CarriedUnresolvedRow> = {
                let mut stmt = tx.prepare(&sql)?;
                let mapped = stmt.query_map(
                    rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
                    |row| {
                        Ok(CarriedUnresolvedRow {
                            old_edge_id: row.get(0)?,
                            old_source_symbol_id: row.get(1)?,
                            file_id: row.get(2)?,
                            edge_kind: row.get(3)?,
                            reference_name: row.get(4)?,
                            name_tail: row.get(5)?,
                            reason: row.get(6)?,
                            import_candidates: row.get(7)?,
                            detail: row.get(8)?,
                            evidence_snippet: row.get(9)?,
                            evidence_start_line: row.get(10)?,
                            evidence_end_line: row.get(11)?,
                            confidence: row.get(12)?,
                            commit_sha: row.get(13)?,
                            trace_id: row.get(14)?,
                            span_id: row.get(15)?,
                            event_ts: row.get(16)?,
                            receiver_type: row.get(17)?,
                            bare_call: row.get(18)?,
                            call_shape: row.get(19)?,
                            receiver_scope: row.get(20)?,
                            deferred_kind: row.get(21)?,
                            deferred: row.get(22)?,
                        })
                    },
                )?;
                mapped.collect::<rusqlite::Result<Vec<_>>>()?
            };

            let mut remap_symbol_stmt = tx.prepare(
                "SELECT ns.id FROM symbols os
                 JOIN symbols ns ON ns.stable_id = os.stable_id AND ns.file_id = os.file_id
                 WHERE os.id = ? AND ns.graph_version = ? ORDER BY ns.id LIMIT 1",
            )?;
            let mut insert_unresolved = tx.prepare(resolver::UNRESOLVED_REFERENCE_INSERT_SQL)?;

            for row in rows {
                let CarriedUnresolvedRow {
                    old_edge_id,
                    old_source_symbol_id,
                    file_id,
                    edge_kind,
                    reference_name,
                    name_tail,
                    reason,
                    import_candidates,
                    detail,
                    evidence_snippet,
                    evidence_start_line,
                    evidence_end_line,
                    confidence,
                    commit_sha,
                    trace_id,
                    span_id,
                    event_ts,
                    receiver_type,
                    bare_call,
                    call_shape,
                    receiver_scope,
                    deferred_kind,
                    deferred,
                } = row;
                // Not expected to miss (`old_edge_id` came straight from
                // this same file set's edges), but skip rather than panic
                // on a stale/foreign-key-orphaned store row.
                let Some(&new_edge_id) = edge_id_map.get(&old_edge_id) else {
                    continue;
                };
                let new_source_symbol_id: Option<i64> = match old_source_symbol_id {
                    Some(old_id) => remap_symbol_stmt
                        .query_row(params![old_id, to_version], |row| row.get(0))
                        .optional()?,
                    None => None,
                };
                insert_unresolved.execute(params![
                    new_edge_id,
                    new_source_symbol_id,
                    file_id,
                    edge_kind,
                    reference_name,
                    name_tail,
                    reason,
                    import_candidates,
                    detail,
                    evidence_snippet,
                    evidence_start_line,
                    evidence_end_line,
                    confidence,
                    commit_sha,
                    trace_id,
                    span_id,
                    event_ts,
                    receiver_type,
                    bare_call,
                    call_shape,
                    to_version,
                    receiver_scope,
                    deferred_kind,
                    deferred,
                ])?;
            }
        }

        tx.commit()?;
        Ok(ReferencesCarried {
            stubs: stubs_copied,
            edges: edges_copied,
        })
    }

    /// Delete `symbols`/`edges` rows for every graph version older than the
    /// `keep` most recent ones (see `DEFAULT_GRAPH_VERSION_RETENTION` for why
    /// `keep` is safe to set below the total version count). `symbol_metrics`
    /// rows for pruned symbols are removed via `ON DELETE CASCADE` (foreign
    /// keys are enabled on every connection, see `Db::new`).
    ///
    /// `unresolved_references` rows for those versions are deleted
    /// explicitly rather than left to cascade: a Bridge Edge kind row's
    /// `edge_id` cascades when its edge is deleted above, but a pending
    /// (non-Bridge-Edge-kind) row has `edge_id = NULL` (issue #79's
    /// self-contained store) and no edge to cascade from, and
    /// `carry_forward_references` copies every pending row forward into each new
    /// version -- without this, pruned versions' pending rows would never be
    /// reclaimed and the store would grow unbounded across reindexes.
    ///
    /// `graph_versions` (the id/created/commit_sha metadata rows), `files`,
    /// and `co_changes` are untouched: none of them are duplicated per
    /// reindex the way `symbols`/`edges`/`unresolved_references` are, so none
    /// contribute to the unbounded growth this prunes.
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
                "SELECT id FROM graph_versions WHERE status = ? ORDER BY id DESC LIMIT 1 OFFSET ?",
                params![GV_COMPLETE, keep - 1],
                |row| row.get(0),
            )
            .optional()?;
        let Some(boundary) = boundary else {
            // Fewer than `keep` versions exist yet; nothing to prune.
            return Ok((0, 0, 0));
        };

        let versions_pruned: i64 = tx.query_row(
            "SELECT COUNT(*) FROM graph_versions WHERE id < ? AND status = ?",
            params![boundary, GV_COMPLETE],
            |row| row.get(0),
        )?;
        let edges_deleted = tx.execute(
            "DELETE FROM edges WHERE graph_version < ?",
            params![boundary],
        )?;
        // Before `symbols`: `source_symbol_id` is `ON DELETE SET NULL`, so
        // deleting symbols first would collapse rows that differ only by
        // source symbol onto one `COALESCE(source_symbol_id,-1)` key of
        // `idx_unresolved_references_identity` and violate its UNIQUE.
        tx.execute(
            "DELETE FROM unresolved_references WHERE graph_version < ?",
            params![boundary],
        )?;
        tx.execute(
            "DELETE FROM py_decls WHERE graph_version < ?",
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

        // PHASE 1: DELETE removed symbols (by stable_id). Any edge --
        // in this file or another -- that still references one of these
        // rowids gets nulled automatically by `edges`' `ON DELETE SET
        // NULL` foreign key (issue #76); no manual nulling needed here.
        delete_file_symbols_by_stable_id(&tx, file_id, graph_version, &diff.deleted)?;

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
                 WHERE stable_id = ? AND graph_version = ? AND file_id = ?",
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
                    file_id,
                ])?;

                // Fetch the updated symbol to get its ID
                let id: i64 = tx.query_row(
                    "SELECT id FROM symbols
                     WHERE stable_id = ? AND graph_version = ? AND file_id = ?",
                    params![&stable_id, graph_version, file_id],
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
                     WHERE stable_id = ? AND graph_version = ? AND file_id = ?",
                    params![&stable_id, graph_version, file_id],
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
                            signature: crate::model::public_signature(row.get(10)?),
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

        // PHASE 1: DELETE removed symbols, one file at a time (see the helper).
        for fd in file_diffs {
            delete_file_symbols_by_stable_id(&tx, fd.file_id, fd.graph_version, &fd.diff.deleted)?;
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
                 WHERE stable_id = ? AND graph_version = ? AND file_id = ?",
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
                        fd.file_id,
                    ])?;

                    // Fetch the updated symbol
                    let id: i64 = tx.query_row(
                        "SELECT id FROM symbols
                         WHERE stable_id = ? AND graph_version = ? AND file_id = ?",
                        params![&stable_id, fd.graph_version, fd.file_id],
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
                         WHERE stable_id = ? AND graph_version = ? AND file_id = ?",
                        params![&stable_id, fd.graph_version, fd.file_id],
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
                                signature: crate::model::public_signature(row.get(10)?),
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
                  receiver_type, resolution_kind, import_candidates, bare_call, call_shape,
                  receiver_scope, deferred_kind, deferred)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            let mut exact_lookup_stmt = tx.prepare(
                "SELECT id FROM symbols WHERE qualname = ? AND graph_version = ? ORDER BY id ASC LIMIT 1",
            )?;
            let mut span_lookup_stmt = tx.prepare(
                "SELECT id FROM symbols
                 WHERE file_id = ? AND qualname = ? AND start_byte = ? AND graph_version = ?
                 LIMIT 1",
            )?;
            // Issue #78: one row per `Unresolved` outcome, so
            // `Db::retry_unresolved_references` can retry it later without
            // rescanning every NULL-target edge.
            let mut unresolved_insert_stmt =
                tx.prepare(resolver::UNRESOLVED_REFERENCE_INSERT_SQL)?;
            let mut resolver = resolver::Resolver::new(&tx, graph_version)?;
            // Look up the source file's language and path — same-language
            // preference and the guarded name-fallback's visibility check
            // (`resolver::Reference::source_file_path`) respectively.
            let (source_lang, source_file_path): (String, String) = tx
                .query_row(
                    "SELECT language, path FROM files WHERE id = ?",
                    params![file_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .unwrap_or_else(|_| ("unknown".to_string(), String::new()));

            for edge in edges {
                // An overload-pinned source is the symbol at that span.
                let pinned = match (&edge.source_qualname, edge.source_start_byte) {
                    (Some(qualname), Some(start_byte)) => span_lookup_stmt
                        .query_row(
                            params![file_id, qualname, start_byte, graph_version],
                            |row| row.get(0),
                        )
                        .optional()?,
                    _ => None,
                };
                let source_id = match pinned {
                    Some(id) => Some(id),
                    None => resolve_symbol_id(
                        &edge.source_qualname,
                        symbol_map,
                        &mut exact_lookup_stmt,
                        graph_version,
                    )?,
                };
                // A channel edge with no source symbol is unreachable from
                // either end (#222); fail loudly instead of storing it.
                if source_id.is_none() && edge.kind.starts_with("CHANNEL_") {
                    anyhow::bail!(
                        "{} edge has unresolved source {:?} (target {:?})",
                        edge.kind,
                        edge.source_qualname,
                        edge.target_qualname
                    );
                }
                let receiver = edge.receiver_type.to_columns();
                let extracted_receiver_type = receiver.receiver_type.as_deref();
                let marker = edge.receiver_type.deferred_marker();
                let scope = match &edge.receiver_type {
                    ReceiverType::Scoped { scope, .. } => Some(scope),
                    _ => None,
                };
                let call_shape = edge.call_shape.map(|shape| shape.encode());
                let pinned_target = match (&edge.target_qualname, edge.target_start_byte) {
                    (Some(qualname), Some(start_byte)) => span_lookup_stmt
                        .query_row(
                            params![file_id, qualname, start_byte, graph_version],
                            |row| row.get(0),
                        )
                        .optional()?,
                    _ => None,
                };
                let resolution = resolver.resolve(
                    &resolver::Reference {
                        target_qualname: edge.target_qualname.as_deref(),
                        edge_kind: &edge.kind,
                        receiver_type: extracted_receiver_type,
                        receiver_scope: scope,
                        deferred: marker.as_ref(),
                        import_candidates: &edge.import_candidates,
                        source_lang: &source_lang,
                        source_file_path: &source_file_path,
                        source_qualname: edge.source_qualname.as_deref(),
                        source_symbol_id: source_id,
                        bare_call: edge.bare_call,
                        call_shape: edge.call_shape,
                    },
                    symbol_map,
                )?;
                let resolution = match pinned_target {
                    Some(target_id) => resolver::Resolution::Resolved {
                        target_id,
                        kind: resolver::ResolutionKind::Exact,
                    },
                    None => resolution,
                };

                // Issue #79: `is_bridge_edge_kind`'s kind is always written,
                // resolved or not -- see its doc for why that's not one
                // uniform reason (the three actual Bridge Edge pairs need
                // `target_qualname` for trace_flow's traversal bridging;
                // CONFIG_SOURCE/CONFIG_READ/CONFIG_BIND for config-URI
                // lookups; XREF for confidence-gated consumers that read the
                // edge's text directly). Every other kind is written only
                // when resolved; an Unresolved outcome for one of those has
                // no placeholder edge at all, only the `unresolved_references`
                // row below.
                let is_bridge = crate::indexer::channel::is_bridge_edge_kind(&edge.kind);
                let stored_target = match resolution.target_id() {
                    Some(id) => resolver::bound_target_qualname(
                        &tx,
                        matches!(marker, Some(DeferredMarker::Argument(_))),
                        edge.target_qualname.as_deref(),
                        id,
                    )?,
                    None => edge.target_qualname.clone(),
                };
                let edge_id = if resolution.target_id().is_some() || is_bridge {
                    insert_stmt.execute(params![
                        file_id,
                        source_id,
                        resolution.target_id(),
                        &edge.kind,
                        stored_target.as_deref(),
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
                        resolution.stored_receiver_type(extracted_receiver_type, marker.is_some()),
                        resolution.kind_column(),
                        resolver::encode_import_candidates(&edge.import_candidates),
                        edge.bare_call,
                        call_shape.as_deref(),
                        receiver.receiver_scope.as_deref(),
                        receiver.deferred_kind,
                        receiver.deferred.as_deref(),
                    ])?;
                    count += 1;
                    Some(tx.last_insert_rowid())
                } else {
                    None
                };

                if let Some(reason) = resolution.unresolved_reason()
                    && let Some((reference_name, name_tail)) =
                        resolver::store_reference_name_and_tail(
                            edge.target_qualname.as_deref(),
                            &edge.import_candidates,
                        )
                {
                    unresolved_insert_stmt.execute(params![
                        edge_id,
                        source_id,
                        file_id,
                        &edge.kind,
                        reference_name,
                        name_tail,
                        reason.as_str(),
                        resolver::encode_import_candidates(&edge.import_candidates),
                        edge.detail.as_deref(),
                        edge.evidence_snippet.as_deref(),
                        edge.evidence_start_line,
                        edge.evidence_end_line,
                        edge.confidence,
                        commit_sha,
                        edge.trace_id.as_deref(),
                        edge.span_id.as_deref(),
                        edge.event_ts,
                        resolution.stored_receiver_type(extracted_receiver_type, marker.is_some()),
                        edge.bare_call,
                        call_shape.as_deref(),
                        graph_version,
                        receiver.receiver_scope.as_deref(),
                        receiver.deferred_kind,
                        receiver.deferred.as_deref(),
                    ])?;
                }
            }
        }
        tx.commit()?;
        Ok(count)
    }

    /// Set `symbols.visibility` for `file_id`'s symbols in `graph_version`
    /// from `private_qualnames` (see `ExtractedFile::private_qualnames`):
    /// `'private'` for a qualname in the list, `NULL` (unrestricted) for
    /// every other symbol in the file. Always resets the whole file's
    /// symbols in one statement — not just the ones currently in the
    /// list — so an incremental re-index that removes a `pub`/`private`
    /// modifier clears the stale mark rather than leaving it from the
    /// previous extraction.
    ///
    /// Called once per file, after that file's symbols are inserted/
    /// updated and before its edges are resolved (visibility is a
    /// cross-file resolver input — see `db::resolver::VisibilityRule`).
    pub fn set_private_symbols(
        &mut self,
        file_id: i64,
        graph_version: i64,
        private_qualnames: &[String],
        static_member_qualnames: &[String],
        override_symbols: &[(String, i64)],
    ) -> Result<()> {
        if private_qualnames.is_empty()
            && static_member_qualnames.is_empty()
            && override_symbols.is_empty()
        {
            self.conn().execute(
                "UPDATE symbols SET visibility = NULL
                 WHERE file_id = ? AND graph_version = ? AND visibility IS NOT NULL",
                params![file_id, graph_version],
            )?;
            return Ok(());
        }
        // `visibility` is a space-separated modifier list: `private`, `static`.
        let private_ph = vec!["?"; private_qualnames.len()].join(",");
        let static_ph = vec!["?"; static_member_qualnames.len()].join(",");
        // Overloads share a qualname, so an override is keyed by its line too.
        let override_test = if override_symbols.is_empty() {
            "0".to_string()
        } else {
            format!(
                "(qualname, start_line) IN (VALUES {})",
                vec!["(?,?)"; override_symbols.len()].join(",")
            )
        };
        let sql = format!(
            "UPDATE symbols
                SET visibility = NULLIF(TRIM(
                    CASE WHEN qualname IN ({private_ph}) THEN 'private' ELSE '' END
                    || CASE WHEN qualname IN ({static_ph}) THEN ' static' ELSE '' END
                    || CASE WHEN {override_test} THEN ' override' ELSE '' END), '')
             WHERE file_id = ? AND graph_version = ?"
        );
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = private_qualnames
            .iter()
            .chain(static_member_qualnames)
            .map(|q| Box::new(q.clone()) as Box<dyn rusqlite::ToSql>)
            .collect();
        for (qualname, line) in override_symbols {
            params.push(Box::new(qualname.clone()));
            params.push(Box::new(*line));
        }
        params.push(Box::new(file_id));
        params.push(Box::new(graph_version));
        self.conn().execute(
            &sql,
            rusqlite::params_from_iter(params.iter().map(|p| p.as_ref())),
        )?;
        Ok(())
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
        symbols: &[Symbol],
    ) -> Result<usize> {
        if metrics.is_empty() {
            return Ok(0);
        }
        // Keyed by (qualname, start byte), not qualname alone: declarations
        // sharing a qualname (C# overloads, cfg twins) each own their metric.
        let by_span: HashMap<(&str, i64), i64> = symbols
            .iter()
            .map(|s| ((s.qualname.as_str(), s.start_byte), s.id))
            .collect();
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
                let Some(symbol_id) = by_span.get(&(metric.qualname.as_str(), metric.start_byte))
                else {
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

    /// Every symbol sharing `qualname` (an overload set, e.g. C# methods
    /// with different parameter lists), in source order.
    pub fn get_symbols_by_qualname(
        &self,
        qualname: &str,
        graph_version: i64,
    ) -> Result<Vec<Symbol>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.qualname = ?
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
             ORDER BY f.path, s.start_line",
        )?;
        let rows = stmt.query_map(
            params![qualname, graph_version, graph_version],
            symbol_from_row,
        )?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
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
    /// `(path, module qualname)` of every Python file's module symbol in
    /// `graph_version`.
    pub fn python_module_names(&self, graph_version: i64) -> Result<Vec<(String, String)>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT f.path, s.qualname FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE f.language = 'python' AND s.kind = 'module' AND s.graph_version = ?",
        )?;
        let rows = stmt.query_map(params![graph_version], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(rows.collect::<rusqlite::Result<_>>()?)
    }

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
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.resolution_kind
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        sql.push_str(RPC_NAME_ONLY_FILTER);
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

    /// Allocate a new graph version in the `building` state. It is not
    /// current: `current_graph_version` keeps reporting the last completed
    /// version until `promote_graph_version` runs, so readers and any other
    /// reindex never see a half-populated version (issue #250).
    pub fn allocate_graph_version(&self, commit_sha: Option<&str>) -> Result<i64> {
        let created = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let conn = self.conn();
        conn.execute(
            "INSERT INTO graph_versions (created, commit_sha, status) VALUES (?, ?, ?)",
            params![created, commit_sha, GV_BUILDING],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Mark a `building` version complete and make it current, atomically.
    pub fn promote_graph_version(&self, id: i64) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let updated = tx.execute(
            "UPDATE graph_versions SET status = ? WHERE id = ?",
            params![GV_COMPLETE, id],
        )?;
        if updated == 0 {
            anyhow::bail!("cannot promote unknown graph version {id}");
        }
        tx.execute(
            "INSERT INTO meta (key, value) VALUES ('graph_version', ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![id.to_string()],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Allocate and immediately promote an (empty) version (test fixtures).
    #[cfg(test)]
    pub fn create_graph_version(&self, commit_sha: Option<&str>) -> Result<i64> {
        let id = self.allocate_graph_version(commit_sha)?;
        self.promote_graph_version(id)?;
        Ok(id)
    }

    /// Delete every version left `building` by a reindex that died before
    /// promoting it (rows in `symbols`, `edges`, `unresolved_references`, and
    /// the `graph_versions` row). Requires the reindex lock as proof no live
    /// reindex owns a `building` version. Returns the versions reclaimed.
    pub fn reclaim_abandoned_graph_versions(&self, _lock: &ReindexLock) -> Result<usize> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let ids: Vec<i64> = {
            let mut stmt = tx.prepare("SELECT id FROM graph_versions WHERE status != ?")?;
            stmt.query_map(params![GV_COMPLETE], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?
        };
        for id in &ids {
            tx.execute("DELETE FROM edges WHERE graph_version = ?", params![id])?;
            tx.execute("DELETE FROM symbols WHERE graph_version = ?", params![id])?;
            tx.execute("DELETE FROM py_decls WHERE graph_version = ?", params![id])?;
            tx.execute(
                "DELETE FROM unresolved_references WHERE graph_version = ?",
                params![id],
            )?;
            tx.execute("DELETE FROM graph_versions WHERE id = ?", params![id])?;
            // Undo the file deletions it recorded: the completed version
            // still contains those files.
            tx.execute(
                "UPDATE files SET deleted_version = NULL WHERE deleted_version = ?",
                params![id],
            )?;
        }
        tx.commit()?;
        Ok(ids.len())
    }

    /// Take the reindex lock, or fail with [`ReindexBusy`] (downcastable from
    /// the returned `anyhow::Error`) when another process holds it. The lock
    /// is an OS advisory lock on a sidecar file, so the kernel drops it when
    /// its holder dies: a killed reindex never leaves a stale lock behind.
    pub fn try_lock_reindex(&self) -> Result<ReindexLock> {
        use std::io::{Read, Seek, Write};
        let mut lock_path = self.db_path.clone().into_os_string();
        lock_path.push(".reindex.lock");
        let lock_path = PathBuf::from(lock_path);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("open reindex lock {}", lock_path.display()))?;
        match file.try_lock() {
            Ok(()) => {
                file.set_len(0)?;
                file.rewind()?;
                write!(file, "{}", std::process::id())?;
                file.flush()?;
                Ok(ReindexLock { _file: file })
            }
            Err(std::fs::TryLockError::WouldBlock) => {
                let mut contents = String::new();
                let _ = file.read_to_string(&mut contents);
                Err(ReindexBusy {
                    holder_pid: contents.trim().parse().ok(),
                    lock_path,
                }
                .into())
            }
            Err(std::fs::TryLockError::Error(err)) => {
                Err(err).with_context(|| format!("lock {}", lock_path.display()))
            }
        }
    }

    pub fn list_graph_versions(&self, limit: usize, offset: usize) -> Result<Vec<GraphVersion>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, created, commit_sha
             FROM graph_versions
             WHERE status = ?
             ORDER BY id DESC
             LIMIT ? OFFSET ?",
        )?;
        let limit = limit as i64;
        let offset = offset as i64;
        let rows = stmt.query_map(params![GV_COMPLETE, limit, offset], |row| {
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

    /// Text value of a `meta` row.
    pub fn get_meta_str(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .read_conn()?
            .query_row(
                "SELECT value FROM meta WHERE key = ?",
                params![key],
                |row| row.get(0),
            )
            .optional()?)
    }

    pub fn set_meta_str(&self, key: &str, value: &str) -> Result<()> {
        self.conn().execute(
            "INSERT INTO meta (key, value) VALUES (?, ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    pub fn delete_meta(&self, key: &str) -> Result<()> {
        self.conn()
            .execute("DELETE FROM meta WHERE key = ?", params![key])?;
        Ok(())
    }

    /// Every `meta` row whose key starts with `prefix`, as `(key, value)`.
    pub fn meta_with_prefix(&self, prefix: &str) -> Result<Vec<(String, String)>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare("SELECT key, value FROM meta WHERE substr(key, 1, ?1) = ?2")?;
        let rows = stmt.query_map(params![prefix.len() as i64, prefix], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
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

/// True when `path` is one of `prefixes` or lies beneath one of them. An
/// empty prefix list means "no filter" and matches everything. Mirrors the
/// SQL emitted by `append_path_filters` (`path = p OR path LIKE 'p/%'`).
fn path_in_prefixes(prefixes: &[String], path: &str) -> bool {
    prefixes.is_empty()
        || prefixes.iter().any(|p| {
            path == p
                || path
                    .strip_prefix(p.as_str())
                    .is_some_and(|rest| rest.starts_with('/'))
        })
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
        signature: crate::model::public_signature(row.get(11)?),
        docstring: row.get(12)?,
        graph_version: row.get(13)?,
        commit_sha: row.get(14)?,
        stable_id: row.get(15)?,
    })
}

/// SQL tail (alias `e`) hiding a TS/JS `RPC_CALL` accepted on its `*Client`
/// name alone (`"evidence":"name"` in `detail`, #204) unless a proto `service`
/// symbol of that name exists in the same graph version. Evaluated at read
/// time, so adding or removing the `.proto` flips the edge exactly as a fresh
/// index would. Likewise an RPC_CALL named `bind`/`call`/`apply` (a possible
/// `Function.prototype` call through a client) surfaces only when a proto
/// route has that method name.
pub(crate) const RPC_NAME_ONLY_FILTER: &str = " AND (e.kind <> 'RPC_CALL'
    OR e.detail IS NULL
    OR e.detail NOT LIKE '%\"evidence\":\"name\"%'
    OR EXISTS (SELECT 1 FROM symbols ps JOIN files pf ON ps.file_id = pf.id
               WHERE ps.kind = 'service'
                 AND ps.name = json_extract(e.detail, '$.service')
                 AND ps.graph_version = e.graph_version
                 AND (pf.deleted_version IS NULL OR pf.deleted_version > e.graph_version)))
    AND (e.kind <> 'RPC_CALL'
    OR e.detail IS NULL
    OR lower(json_extract(e.detail, '$.rpc')) NOT IN ('bind', 'call', 'apply')
    OR EXISTS (SELECT 1 FROM edges r
               WHERE r.kind = 'RPC_ROUTE' AND r.graph_version = e.graph_version
                 AND r.target_qualname LIKE '%/' || lower(json_extract(e.detail, '$.rpc'))))";

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
        resolution_kind: row.get(16)?,
        dispatch_args: None,
    })
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

/// Delete `file_id`'s symbols whose `stable_id` is in `deleted`, after handing
/// shared-namespace edges to a surviving declaration. `stable_id` is
/// content-only (no file component), so several files can share one; the
/// delete is therefore per file (and graph version), never a bare
/// `stable_id IN (...)` across files, which would drop other files' copies.
fn delete_file_symbols_by_stable_id(
    conn: &rusqlite::Connection,
    file_id: i64,
    graph_version: i64,
    deleted: &[String],
) -> Result<()> {
    if deleted.is_empty() {
        return Ok(());
    }
    reown_shared_namespace_edges(conn, file_id, graph_version, Some(deleted))?;
    let placeholders = vec!["?"; deleted.len()].join(",");
    let mut params: Vec<&dyn rusqlite::ToSql> =
        deleted.iter().map(|d| d as &dyn rusqlite::ToSql).collect();
    params.push(&graph_version);
    params.push(&file_id);
    // Edges (any file's) still pointing at a deleted rowid are nulled by
    // `edges`' `ON DELETE SET NULL` foreign key (issue #76).
    conn.execute(
        &format!(
            "DELETE FROM symbols WHERE stable_id IN ({placeholders})
             AND graph_version = ? AND file_id = ?"
        ),
        rusqlite::params_from_iter(params),
    )?;
    Ok(())
}

/// A namespace declared by several files is one symbol per file, and other
/// files' edges bind to the one a fresh index saw first: the file earliest in
/// path order (files are indexed in that order). Before `file_id`'s copies
/// go (all of them, or only those whose `stable_id` is in `only`), hand such
/// edges to that survivor, or they'd be nulled and diverge from a fresh
/// reindex.
fn reown_shared_namespace_edges(
    conn: &rusqlite::Connection,
    file_id: i64,
    graph_version: i64,
    only: Option<&[String]>,
) -> Result<()> {
    let filter = match only {
        Some(ids) => format!("AND stable_id IN ({})", vec!["?"; ids.len()].join(",")),
        None => String::new(),
    };
    for column in ["source_symbol_id", "target_symbol_id"] {
        let survivor = format!(
            "SELECT o.id FROM symbols o
                 JOIN files fo ON fo.id = o.file_id
                 JOIN symbols d ON d.qualname = o.qualname AND d.kind = o.kind
                  AND d.graph_version = o.graph_version
                 WHERE d.id = edges.{column} AND o.file_id != d.file_id
                 ORDER BY fo.path LIMIT 1"
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&file_id, &graph_version];
        if let Some(ids) = only {
            params.extend(ids.iter().map(|i| i as &dyn rusqlite::ToSql));
        }
        conn.execute(
            &format!(
                "UPDATE edges SET {column} = COALESCE(({survivor}), {column})
                 WHERE graph_version = ?2 AND {column} IN (
                    SELECT id FROM symbols
                    WHERE file_id = ?1 AND graph_version = ?2 AND kind = 'namespace' {filter})"
            ),
            rusqlite::params_from_iter(params),
        )?;
    }
    Ok(())
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
            name: resolver::qualname_trailing_name(qualname).to_string(),
            qualname: qualname.to_string(),
            start_line,
            start_col: 0,
            end_line: start_line + 5,
            end_col: 0,
            start_byte: 0,
            end_byte: 100,
            signature: signature.map(String::from),
            docstring: None,
            identity: None,
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
            bare_call: false,
            call_shape: None,
            source_start_byte: None,
            target_start_byte: None,
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

    /// Issue #251: two identical-looking Bridge Edge kind edges are two edges;
    /// each keeps its own store row bound to it (the identity index only
    /// covers pending rows), so neither loses its retry binding.
    #[test]
    fn identical_unresolved_bridge_edges_each_keep_a_bound_store_row() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("pkg/a.py", "h1", "python", 100, 0).unwrap();
        let symbols = vec![make_test_symbol(
            "pkg.a.caller",
            Some("def caller()"),
            "function",
            1,
        )];
        let inserted = db
            .insert_symbols(file_id, "pkg/a.py", &symbols, 1, None)
            .unwrap();
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        let edges = vec![
            make_test_edge("HTTP_CALL", "pkg.a.caller", "/nowhere/x"),
            make_test_edge("HTTP_CALL", "pkg.a.caller", "/nowhere/x"),
        ];
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();
        let conn = db.read_conn().unwrap();
        let edge_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE kind = 'HTTP_CALL'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let bound: i64 = conn
            .query_row(
                "SELECT COUNT(DISTINCT edge_id) FROM unresolved_references
                 WHERE edge_kind = 'HTTP_CALL' AND edge_id IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(edge_count, 2);
        assert_eq!(bound, 2, "each edge must keep its own bound store row");
    }

    /// Issue #251: an identical pending reference inserted twice is stored once.
    #[test]
    fn identical_pending_references_are_merged_not_duplicated() {
        let (mut db, _temp) = create_test_db();
        let file_id = db.upsert_file("pkg/a.py", "h1", "python", 100, 0).unwrap();
        let symbols = vec![make_test_symbol(
            "pkg.a.caller",
            Some("def caller()"),
            "function",
            1,
        )];
        let inserted = db
            .insert_symbols(file_id, "pkg/a.py", &symbols, 1, None)
            .unwrap();
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        let edges = vec![make_test_edge("CALLS", "pkg.a.caller", "nowhere_at_all")];
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();
        let rows: i64 = db
            .read_conn()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM unresolved_references", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(rows, 1);
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
    /// that, `symbol_metrics` being dropped wholesale): `carry_forward_references`
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
    /// `carry_forward_references` doesn't copy comes back NULL and fails loudly,
    /// naming exactly the table and column at fault.
    #[test]
    fn carry_forward_copies_every_non_exempt_column() {
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
                    // `carry_forward_references`), not a literal copy of the old
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
            start_byte: inserted[0].start_byte,
            loc: 5,
            complexity: 2,
            duplication_hash: Some("duphash".to_string()),
        }];
        db.insert_symbol_metrics(file_id, &metrics, &inserted)
            .unwrap();

        // Stamp a unique, non-NULL sentinel into every non-exempt column of
        // the one row on each table, so a column `carry_forward_references`
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

        let ids = [file_id];
        let carried = db.carry_forward_symbols(&ids, 1, 2).unwrap();
        db.carry_forward_references(carried).unwrap();

        let new_symbol_id: i64 = db
            .conn()
            .query_row(
                "SELECT id FROM symbols WHERE file_id = ?1 AND graph_version = 2",
                params![file_id],
                |row| row.get(0),
            )
            .expect("carry_forward_references must copy the symbols row to the new graph version");
        let new_edge_id: i64 = db
            .conn()
            .query_row(
                "SELECT id FROM edges WHERE file_id = ?1 AND graph_version = 2",
                params![file_id],
                |row| row.get(0),
            )
            .expect("carry_forward_references must copy the edges row to the new graph version");
        let new_metrics_id: i64 = db
            .conn()
            .query_row(
                "SELECT id FROM symbol_metrics WHERE symbol_id = ?1",
                params![new_symbol_id],
                |row| row.get(0),
            )
            .expect(
                "carry_forward_references must copy the symbol_metrics row to the new graph version",
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
                    "carry_forward_references did not copy `{table}.{column}` into the new graph \
                     version (found {actual:?}, expected the source row's value {expected:?}). \
                     Add `{column}` to both the INSERT column list and the SELECT in \
                     Db::carry_forward_references's `{table}` copy -- or, if `{column}` must \
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
                "carry_forward_references did not preserve `{table}.file_id` on the copied row"
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
                identity: None,
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
            bare_call: false,
            call_shape: None,
            source_start_byte: None,
            target_start_byte: None,
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

        // Confirm the edge really is unresolved (the write path refused it,
        // and issue #79 means it never became an edge at all -- only a
        // store row).
        let edge_count: i64 = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE graph_version = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(edge_count, 0);
        let unresolved_count: i64 = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM unresolved_references WHERE graph_version = 1",
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
        // must not bind this edge to any real symbol (exact match also can't
        // hit: no symbol is named exactly "value.Trim"). No import is
        // involved, so issue #80's known-external stub tier doesn't apply
        // either (that's scoped to imports known to resolve outside the
        // repo) -- this stays unresolved, no edge at all, same as before
        // #80. The load-bearing check below is still that it never
        // resurrects as a caller of the unrelated Python `trim`.
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

        // CALLS isn't a Bridge Edge kind, so a builtin/unresolved receiver
        // with no import involved leaves no edge at all, only an
        // `unresolved_references` row.
        let edge_count: i64 = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE target_qualname = 'value.Trim'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(edge_count, 0);

        let by_symbol = db.edges_for_symbols(&[py_trim_id], None, 1).unwrap();
        assert!(
            by_symbol[&py_trim_id].is_empty(),
            "C# value.Trim() must not resurrect as a caller of Python trim()"
        );
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
            bare_call: false,
            call_shape: None,
            source_start_byte: None,
            target_start_byte: None,
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
                bare_call: false,
                call_shape: None,
                source_start_byte: None,
                target_start_byte: None,
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
                bare_call: false,
                call_shape: None,
                source_start_byte: None,
                target_start_byte: None,
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
        // Only the exact edge: the guessed package contradicts the route's.
        assert_eq!(found.len(), 1);
    }

    /// TS handler (package-less guess) plus a real proto route.
    fn rpc_guess_fixture(guessed: &str, real: &str) -> (Db, tempfile::TempDir) {
        let (mut db, temp) = create_test_db();
        let file_id = db
            .upsert_file("src/svc.ts", "h1", "typescript", 100, 0)
            .unwrap();
        let inserted = db
            .insert_symbols(
                file_id,
                "src/svc.ts",
                &[make_test_symbol("svc.getTables", None, "function", 1)],
                1,
                None,
            )
            .unwrap();
        let symbol_map: HashMap<String, i64> = inserted
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        let edge = |kind: &str, src: Option<&str>, tq: &str| crate::indexer::extract::EdgeInput {
            kind: kind.to_string(),
            source_qualname: src.map(str::to_string),
            target_qualname: Some(tq.to_string()),
            ..Default::default()
        };
        db.insert_edges(
            file_id,
            &[edge("RPC_IMPL", Some("svc.getTables"), guessed)],
            &symbol_map,
            1,
            None,
        )
        .unwrap();
        let proto_id = db.upsert_file("p.proto", "h2", "proto", 10, 0).unwrap();
        let rpc = "datacatalog.v1.DataCatalogService.GetTables";
        let proto_syms = db
            .insert_symbols(
                proto_id,
                "p.proto",
                &[make_test_symbol(rpc, None, "rpc", 1)],
                1,
                None,
            )
            .unwrap();
        let proto_map: HashMap<String, i64> = proto_syms
            .iter()
            .map(|s| (s.qualname.clone(), s.id))
            .collect();
        db.insert_edges(
            proto_id,
            &[edge("RPC_ROUTE", Some(rpc), real)],
            &proto_map,
            1,
            None,
        )
        .unwrap();
        (db, temp)
    }

    const REAL_ROUTE: &str = "/datacatalog.v1.datacatalogservice/gettables";

    #[test]
    fn test_rpc_widening_proto_side_finds_package_less_impl() {
        let (db, _temp) = rpc_guess_fixture("/datacatalogservice/gettables", REAL_ROUTE);
        let found = db
            .edges_by_target_qualname_and_kinds(REAL_ROUTE, &["RPC_IMPL"], None, 1)
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, "RPC_IMPL");
    }

    #[test]
    fn test_rpc_widening_impl_side_finds_proto_route() {
        let guessed = "/datacatalogservice/gettables";
        let (db, _temp) = rpc_guess_fixture(guessed, REAL_ROUTE);
        let found = db
            .edges_by_target_qualname_and_kinds(guessed, &["RPC_ROUTE"], None, 1)
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].kind, "RPC_ROUTE");
    }

    #[test]
    fn test_rpc_guess_with_contradicting_package_does_not_bind() {
        let guessed = "/other.pkg.datacatalogservice/gettables";
        let (db, _temp) = rpc_guess_fixture(guessed, REAL_ROUTE);
        // Neither end reaches the other through the guessed path.
        assert!(
            db.edges_by_target_qualname_and_kinds(REAL_ROUTE, &["RPC_IMPL"], None, 1)
                .unwrap()
                .is_empty()
        );
        assert!(
            db.edges_by_target_qualname_and_kinds(guessed, &["RPC_ROUTE"], None, 1)
                .unwrap()
                .is_empty()
        );
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
            bare_call: false,
            call_shape: None,
            source_start_byte: None,
            target_start_byte: None,
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
                bare_call: false,
                call_shape: None,
                source_start_byte: None,
                target_start_byte: None,
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
                bare_call: false,
                call_shape: None,
                source_start_byte: None,
                target_start_byte: None,
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

        // Issue #79: a CALLS edge that can't resolve (target_qualname matches
        // only a graph_version=1 symbol, id {old id}; it must not bind to
        // that stale row) is not written as an edge at all -- only a store
        // row, with the unresolved qualname preserved for later
        // re-resolution / display.
        let found = db.edges_for_symbol(caller_inserted[0].id, None, 2).unwrap();
        assert_eq!(
            found.len(),
            0,
            "an unresolved CALLS edge must not be written at all (target_qualname matches \
             only a stale graph_version=1 symbol, id {})",
            old_inserted[0].id
        );
        let reference_name: String = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT reference_name FROM unresolved_references WHERE graph_version = 2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reference_name, "crate::gather_context::resolve_seeds");
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

    /// The one edge's resolved target for `source_id`, or `None` for both a
    /// NULL-target edge and (issue #79) an unresolved, non-Bridge-Edge-kind
    /// reference that was never written as an edge at all -- callers use
    /// this to assert "this call didn't bind to anything" either way.
    fn only_target(db: &Db, source_id: i64) -> Option<i64> {
        let found = db.edges_for_symbol(source_id, None, 1).unwrap();
        assert!(found.len() <= 1);
        found.first().and_then(|e| e.target_symbol_id)
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
        // Issue #80: a TS/TSX import-candidate miss is the resolver's
        // known-external tier, so both calls now bind to an external stub
        // (named from the call's own `target_qualname` text here, since
        // neither test edge is a bare call -- see `external_stub_qualname`)
        // instead of staying unresolved. Neither ever guesses a real repo
        // symbol by name, which was always the actual point of this test.
        let a_target = only_target(&db, ids["app/page.A"]).expect("stub target");
        let a_qualname = db.get_symbol_by_id(a_target).unwrap().unwrap().qualname;
        assert_eq!(a_qualname, "ext:app/page.useState");

        let b_target = only_target(&db, ids["app/page.B"]).expect("stub target");
        let b_qualname = db.get_symbol_by_id(b_target).unwrap().unwrap().qualname;
        assert_eq!(b_qualname, "ext:app/page.helper");
        assert!(
            !ids.values().any(|&id| id == a_target || id == b_target),
            "must never guess one of the real repo symbols by name"
        );
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

        // Issue #79: the ambiguous "append" reference gets no edge at all,
        // only a store row -- only the resolved "compute" edge is written.
        let found = db.edges_for_symbol(caller_id, None, 1).unwrap();
        assert_eq!(found.len(), 1);

        let compute_edge = found
            .iter()
            .find(|e| e.target_qualname.as_deref() == Some("compute"))
            .unwrap();
        assert_eq!(
            compute_edge.target_symbol_id,
            Some(compute_id),
            "unambiguous bare-name call must still resolve"
        );

        let unresolved_count: i64 = db
            .read_conn()
            .unwrap()
            .query_row(
                "SELECT COUNT(*) FROM unresolved_references \
                 WHERE graph_version = 1 AND reference_name = 'append'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            unresolved_count, 1,
            "ambiguous bare-name call must not bind to either same-named candidate, \
             and must be tracked in the unresolved-reference store"
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

        // A builtin/unresolved receiver type must never bind to a real repo
        // symbol, even though EventStore.append is the sole candidate for
        // the bare name "append". No import is involved, so issue #80's
        // known-external stub tier doesn't apply either -- this stays
        // unresolved exactly as before #80: CALLS isn't a Bridge Edge kind,
        // so no edge at all, only an `unresolved_references` row, whose own
        // `receiver_type` column still records the tracked-but-unresolved
        // marker (`""`, distinct from NULL/not tracked at all).
        let edge_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE target_qualname = 'cells.append'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            edge_count, 0,
            "must never bind to EventStore.append just because it's the sole bare-name candidate, \
             and must not stub either since no import is involved"
        );
        let (receiver_type, reason): (Option<String>, String) = db
            .conn()
            .query_row(
                "SELECT receiver_type, reason FROM unresolved_references
                 WHERE reference_name = 'cells.append'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            receiver_type.as_deref(),
            Some(""),
            "the receiver_type column encodes tracked-but-unresolved as an empty string, \
             distinct from NULL (not tracked at all)"
        );
        assert_eq!(reason, "external");
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

        // Two import candidates that both name real (but different) symbols
        // must not bind to either, and since both live in this repository
        // the reference is an in-repo ambiguity, never an `ext:` stub
        // (issue #239).
        let edges: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE target_qualname = 'Widget.Create'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(edges, 0, "no edge: neither a real symbol nor a stub");
        let reason: String = db
            .conn()
            .query_row(
                "SELECT reason FROM unresolved_references WHERE reference_name = 'Widget.Create'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(reason, "ambiguous");
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

        // A receiver positively known (via this file's own imports) to come
        // from an external module must never fall through to bare-name
        // matching, even though FakeClock.now is the sole local symbol
        // named "now" -- issue #80: it binds to the external stub instead
        // (`ext:datetime.now`, not `pkg.tests.conftest.FakeClock.now`), and
        // the edge's own `receiver_type` column still records the
        // tracked-but-unresolved marker a repair pass relies on to never
        // fuzzy-resolve it.
        let (target_symbol_id, receiver_type): (Option<i64>, Option<String>) = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, receiver_type FROM edges WHERE target_qualname = 'datetime.now'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_ne!(
            target_symbol_id,
            Some(inserted[0].id),
            "must never bind to FakeClock.now just because it's the sole bare-name candidate"
        );
        let stub_qualname: String = db
            .conn()
            .query_row(
                "SELECT qualname FROM symbols WHERE id = ?",
                [target_symbol_id.expect("known-external binds to a stub, not NULL")],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(stub_qualname, "ext:datetime.now");
        assert_eq!(
            receiver_type.as_deref(),
            Some(""),
            "a failed import candidate must be persisted as tracked-but-unresolved, the same \
             column value a receiver-type-tracked builtin uses, so a later repair pass also \
             refuses to fuzzy-resolve it"
        );
    }

    /// Insert `syms` into one python file, then one CALLS edge carrying
    /// `candidates`; return the stored edge row (`None` when issue #79's
    /// write-path gate refused to write one at all -- an unresolved,
    /// non-Bridge-Edge-kind reference lives only in the store now), that
    /// store row's own `receiver_type` (populated whether or not an edge
    /// exists), and the qualname -> id map.
    #[allow(clippy::type_complexity)]
    fn insert_python_import_call(
        syms: &[(&str, &str)],
        target: &str,
        candidates: &[&str],
    ) -> (
        Option<(Option<i64>, Option<String>, Option<String>)>,
        Option<String>,
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
        // A bare call (`quote(x)`): its target is the same-module guess the
        // extractor always qualifies it with, so the stub is named after the
        // import candidate instead (see `external_stub_qualname`).
        let edges = vec![crate::indexer::extract::EdgeInput {
            bare_call: true,
            ..make_test_edge_with_import_candidates(
                "CALLS",
                "py.pkg.src.pkg.a.caller",
                target,
                candidates.iter().map(|c| c.to_string()).collect(),
            )
        }];
        db.insert_edges(file_id, &edges, &symbol_map, 1, None)
            .unwrap();
        let row: Option<(Option<i64>, Option<String>, Option<String>)> = db
            .conn()
            .query_row(
                "SELECT target_symbol_id, receiver_type, resolution_kind FROM edges WHERE kind = 'CALLS'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .unwrap();
        let store_receiver_type: Option<String> = db
            .conn()
            .query_row(
                "SELECT receiver_type FROM unresolved_references WHERE edge_kind = 'CALLS'",
                [],
                |row| row.get(0),
            )
            .optional()
            .unwrap()
            .flatten();
        (row, store_receiver_type, symbol_map)
    }

    #[test]
    fn test_insert_edges_bare_external_import_shadows_unique_repo_method() {
        // `from urllib.parse import quote; quote(x)` in a repo whose only
        // `quote` is an unrelated method. The external import must shadow
        // the name: no bare-name binding to `MssqlCodeWriter.quote`. Issue
        // #80: it binds to the external stub instead of staying unresolved.
        let (row, store_receiver_type, map) = insert_python_import_call(
            &[
                ("py.pkg.src.pkg", "module"),
                ("py.pkg.src.pkg.writer.MssqlCodeWriter.quote", "method"),
            ],
            "py.pkg.src.pkg.a.quote",
            &["urllib.parse.quote"],
        );
        let (target, receiver_type, kind) =
            row.expect("known-external must still bind, to the stub");
        assert_ne!(
            target,
            Some(map["py.pkg.src.pkg.writer.MssqlCodeWriter.quote"]),
            "external import must not bind a repo symbol"
        );
        assert_eq!(kind.as_deref(), Some("external"));
        assert_eq!(receiver_type.as_deref(), Some(""));
        // No `unresolved_references` store row either -- it's resolved now.
        assert_eq!(store_receiver_type, None);
    }

    #[test]
    fn test_insert_edges_import_candidate_matches_src_layout_qualname_by_suffix() {
        // dpb shape: module qualnames carry the repo path prefix
        // (`py.pkg.src.`) the import statement does not.
        let (row, _, map) = insert_python_import_call(
            &[
                ("py.pkg.src.pkg", "module"),
                ("py.pkg.src.pkg.runtime.run", "function"),
                ("py.other.src.other.run", "function"),
            ],
            "py.pkg.src.pkg.a.run",
            &["pkg.runtime.run"],
        );
        let (target, _, kind) = row.expect("a resolved reference must be written as an edge");
        assert_eq!(target, Some(map["py.pkg.src.pkg.runtime.run"]));
        assert_eq!(kind.as_deref(), Some("import"));
    }

    #[test]
    fn test_insert_edges_unresolved_repo_import_keeps_fuzzy_fallback() {
        // `from pkg import helper` where `pkg/__init__.py` re-exports
        // `helper` from `pkg.core`: the candidate names nothing, but its
        // root is a repo package, so the pre-existing bare-name tier still
        // runs instead of the edge being refused as external.
        let (row, _, map) = insert_python_import_call(
            &[
                ("py.pkg.src.pkg", "module"),
                ("py.pkg.src.pkg.core.helper", "function"),
            ],
            "py.pkg.src.pkg.a.helper",
            &["pkg.helper"],
        );
        let (target, receiver_type, kind) =
            row.expect("a resolved reference must be written as an edge");
        assert_eq!(target, Some(map["py.pkg.src.pkg.core.helper"]));
        assert_eq!(kind.as_deref(), Some("bare_name"));
        assert_eq!(receiver_type, None);
    }

    #[test]
    fn test_insert_edges_generated_pb2_import_under_repo_package_is_external() {
        // `from pkg.v1 import pkg_pb2 as pb; pb.ColumnDef(...)`: the pb2
        // module is protoc output, so the repo dataclass of the same name
        // must not be picked up by the fuzzy tiers. Issue #80: it binds to
        // the external stub instead of staying unresolved.
        let (row, _, map) = insert_python_import_call(
            &[
                ("py.pkg.src.pkg", "module"),
                ("py.pkg.src.pkg.schema.ColumnDef", "class"),
            ],
            "pb.ColumnDef",
            &["pkg.v1.pkg_pb2.ColumnDef"],
        );
        let (target, _, kind) = row.expect("known-external must still bind, to the stub");
        assert_ne!(target, Some(map["py.pkg.src.pkg.schema.ColumnDef"]));
        assert_eq!(kind.as_deref(), Some("external"));
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

        // Issue #79: two unrelated ancestors declaring the same method is
        // ambiguous and must not bind -- and, since CALLS isn't a Bridge
        // Edge kind, no edge is written at all, only a store row.
        let edge_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM edges WHERE target_qualname = 'foo.method'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(edge_count, 0);
        let unresolved_count: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM unresolved_references WHERE reference_name = 'foo.method'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(unresolved_count, 1);
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
}
