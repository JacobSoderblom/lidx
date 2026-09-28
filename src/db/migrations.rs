use anyhow::{Result, bail};
use rusqlite::{Connection, OptionalExtension, params};

pub const SCHEMA_VERSION: i64 = 18;

pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        BEGIN;
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS graph_versions (
            id INTEGER PRIMARY KEY,
            created INTEGER NOT NULL,
            commit_sha TEXT
        );

        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            path TEXT NOT NULL UNIQUE,
            hash TEXT NOT NULL,
            language TEXT NOT NULL,
            size INTEGER NOT NULL,
            modified INTEGER NOT NULL,
            deleted_version INTEGER
        );

        CREATE TABLE IF NOT EXISTS symbols (
            id INTEGER PRIMARY KEY,
            file_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            name TEXT NOT NULL,
            qualname TEXT NOT NULL,
            start_line INTEGER NOT NULL,
            start_col INTEGER NOT NULL,
            end_line INTEGER NOT NULL,
            end_col INTEGER NOT NULL,
            start_byte INTEGER NOT NULL,
            end_byte INTEGER NOT NULL,
            signature TEXT,
            docstring TEXT,
            graph_version INTEGER NOT NULL DEFAULT 1,
            commit_sha TEXT,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_symbols_name ON symbols(name);
        CREATE INDEX IF NOT EXISTS idx_symbols_qualname ON symbols(qualname);
        CREATE INDEX IF NOT EXISTS idx_symbols_file ON symbols(file_id);

        CREATE TABLE IF NOT EXISTS edges (
            id INTEGER PRIMARY KEY,
            file_id INTEGER NOT NULL,
            source_symbol_id INTEGER,
            target_symbol_id INTEGER,
            kind TEXT NOT NULL,
            target_qualname TEXT,
            detail TEXT,
            evidence_snippet TEXT,
            evidence_start_line INTEGER,
            evidence_end_line INTEGER,
            confidence REAL,
            graph_version INTEGER NOT NULL DEFAULT 1,
            commit_sha TEXT,
            trace_id TEXT,
            span_id TEXT,
            event_ts INTEGER,
            receiver_type TEXT,
            resolution_kind TEXT,
            import_candidates TEXT,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_symbol_id);
        CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_symbol_id);
        CREATE INDEX IF NOT EXISTS idx_edges_file ON edges(file_id);

        CREATE TABLE IF NOT EXISTS file_metrics (
            id INTEGER PRIMARY KEY,
            file_id INTEGER NOT NULL UNIQUE,
            loc INTEGER NOT NULL,
            blank INTEGER NOT NULL,
            comment INTEGER NOT NULL,
            code INTEGER NOT NULL,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_file_metrics_file ON file_metrics(file_id);

        CREATE TABLE IF NOT EXISTS symbol_metrics (
            id INTEGER PRIMARY KEY,
            symbol_id INTEGER NOT NULL UNIQUE,
            file_id INTEGER NOT NULL,
            loc INTEGER NOT NULL,
            complexity INTEGER NOT NULL,
            duplication_hash TEXT,
            FOREIGN KEY(symbol_id) REFERENCES symbols(id) ON DELETE CASCADE,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_symbol_metrics_file ON symbol_metrics(file_id);
        CREATE INDEX IF NOT EXISTS idx_symbol_metrics_complexity ON symbol_metrics(complexity);
        CREATE INDEX IF NOT EXISTS idx_symbol_metrics_dup ON symbol_metrics(duplication_hash);

        COMMIT;
        ",
    )?;

    let existing: Option<i64> = conn
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| {
                row.get::<_, String>(0)
                    .map(|v| v.parse::<i64>().unwrap_or(0))
            },
        )
        .optional()?;

    let existing = existing.unwrap_or(0);

    if existing < 2 {
        if !has_column(conn, "symbols", "start_byte")? {
            conn.execute(
                "ALTER TABLE symbols ADD COLUMN start_byte INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        if !has_column(conn, "symbols", "end_byte")? {
            conn.execute(
                "ALTER TABLE symbols ADD COLUMN end_byte INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
    }

    if existing < 3 && !has_column(conn, "edges", "evidence_snippet")? {
        conn.execute("ALTER TABLE edges ADD COLUMN evidence_snippet TEXT", [])?;
    }

    if existing < 6 {
        if !has_column(conn, "edges", "evidence_start_line")? {
            conn.execute(
                "ALTER TABLE edges ADD COLUMN evidence_start_line INTEGER",
                [],
            )?;
        }
        if !has_column(conn, "edges", "evidence_end_line")? {
            conn.execute("ALTER TABLE edges ADD COLUMN evidence_end_line INTEGER", [])?;
        }
        if !has_column(conn, "edges", "confidence")? {
            conn.execute("ALTER TABLE edges ADD COLUMN confidence REAL", [])?;
        }
    }

    if existing < 7 {
        conn.execute(
            "CREATE TABLE IF NOT EXISTS graph_versions (
                id INTEGER PRIMARY KEY,
                created INTEGER NOT NULL,
                commit_sha TEXT
            )",
            [],
        )?;
        if !has_column(conn, "files", "deleted_version")? {
            conn.execute("ALTER TABLE files ADD COLUMN deleted_version INTEGER", [])?;
        }
        if !has_column(conn, "symbols", "graph_version")? {
            conn.execute(
                "ALTER TABLE symbols ADD COLUMN graph_version INTEGER NOT NULL DEFAULT 1",
                [],
            )?;
        }
        if !has_column(conn, "symbols", "commit_sha")? {
            conn.execute("ALTER TABLE symbols ADD COLUMN commit_sha TEXT", [])?;
        }
        if !has_column(conn, "edges", "graph_version")? {
            conn.execute(
                "ALTER TABLE edges ADD COLUMN graph_version INTEGER NOT NULL DEFAULT 1",
                [],
            )?;
        }
        if !has_column(conn, "edges", "commit_sha")? {
            conn.execute("ALTER TABLE edges ADD COLUMN commit_sha TEXT", [])?;
        }
        if !has_column(conn, "edges", "trace_id")? {
            conn.execute("ALTER TABLE edges ADD COLUMN trace_id TEXT", [])?;
        }
        if !has_column(conn, "edges", "span_id")? {
            conn.execute("ALTER TABLE edges ADD COLUMN span_id TEXT", [])?;
        }
        if !has_column(conn, "edges", "event_ts")? {
            conn.execute("ALTER TABLE edges ADD COLUMN event_ts INTEGER", [])?;
        }
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_edges_trace ON edges(trace_id)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_edges_event_ts ON edges(event_ts)",
            [],
        )?;

        let current_version: Option<i64> = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'graph_version'",
                [],
                |row| {
                    row.get::<_, String>(0)
                        .map(|v| v.parse::<i64>().unwrap_or(1))
                },
            )
            .optional()?;
        let current_version = current_version.unwrap_or(1);
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('graph_version', ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [current_version.to_string()],
        )?;
        let has_versions: Option<i64> = conn
            .query_row("SELECT id FROM graph_versions LIMIT 1", [], |row| {
                row.get(0)
            })
            .optional()?;
        if has_versions.is_none() {
            let created: Option<i64> = conn
                .query_row(
                    "SELECT value FROM meta WHERE key = 'last_indexed'",
                    [],
                    |row| {
                        row.get::<_, String>(0)
                            .map(|v| v.parse::<i64>().unwrap_or(0))
                    },
                )
                .optional()?;
            let created = created.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64
            });
            conn.execute(
                "INSERT INTO graph_versions (id, created, commit_sha) VALUES (?, ?, NULL)",
                params![current_version, created],
            )?;
        }
    }

    if existing < 8 {
        // Migration 8 previously created embedding tables (now removed)
    }

    if existing < 9 {
        // Add stable_id column for content-based symbol identification
        // This enables incremental indexing by tracking symbols across code moves
        if !has_column(conn, "symbols", "stable_id")? {
            conn.execute("ALTER TABLE symbols ADD COLUMN stable_id TEXT", [])?;
        }
        // Create index for fast stable_id lookups
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbols_stable_id ON symbols(stable_id)",
            [],
        )?;
    }

    if existing < 10 {
        // Add target_qualname index for cross-file impact resolution
        // This speeds up fuzzy resolution of unresolved edges
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_edges_target_qualname ON edges(target_qualname)",
            [],
        )?;
        // Add composite index for symbol fuzzy matching by name and kind
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbols_name_kind ON symbols(name, kind)",
            [],
        )?;
    }

    if existing < 11 {
        // Add co_changes table for git co-change intelligence
        // Tracks file pairs that frequently change together in git history
        conn.execute(
            "CREATE TABLE IF NOT EXISTS co_changes (
                id INTEGER PRIMARY KEY,
                file_a TEXT NOT NULL,
                file_b TEXT NOT NULL,
                co_change_count INTEGER NOT NULL DEFAULT 0,
                total_commits_a INTEGER NOT NULL DEFAULT 0,
                total_commits_b INTEGER NOT NULL DEFAULT 0,
                confidence REAL NOT NULL DEFAULT 0.0,
                last_commit_sha TEXT,
                last_commit_ts INTEGER,
                mined_at INTEGER NOT NULL,
                UNIQUE(file_a, file_b)
            )",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_co_changes_file_a ON co_changes(file_a)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_co_changes_file_b ON co_changes(file_b)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_co_changes_confidence ON co_changes(confidence DESC)",
            [],
        )?;
    }

    if existing < 12 {
        conn.execute("DROP TABLE IF EXISTS diagnostics", [])?;
    }

    if existing < 13 {
        // receiver_type: the extractor's receiver-type signal for a CALLS
        // edge (NULL = not tracked/legacy tiers, '' = tracked but
        // unresolved/builtin, other = the inferred type name). Persisted
        // (not just used transiently at insert time) so `resolve_null_target_edges`
        // re-resolves a repaired edge under the same gating on later passes.
        // resolution_kind: provenance for HOW target_symbol_id was resolved
        // ('exact' | 'receiver_type' | 'two_segment' | 'bare_name'), NULL
        // when unresolved. Deliberately separate from `confidence`, which
        // keeps its pre-existing meaning (Rust CALLS extraction certainty).
        if !has_column(conn, "edges", "receiver_type")? {
            conn.execute("ALTER TABLE edges ADD COLUMN receiver_type TEXT", [])?;
        }
        if !has_column(conn, "edges", "resolution_kind")? {
            conn.execute("ALTER TABLE edges ADD COLUMN resolution_kind TEXT", [])?;
        }
    }

    if existing < 14 {
        // import_candidates: the extractor's import-qualified candidate
        // qualnames for this CALLS edge's receiver (see
        // `EdgeInput::import_candidates`), JSON-encoded as `["a.b", ...]`,
        // NULL when the extractor produced none (the common case — only a
        // dotted `X.method()` call whose receiver import-resolves gets
        // any). Previously a transient, unpersisted field: an edge whose
        // import tier failed only because its target file hadn't been
        // carried forward yet (see `carry_forward_files`, which runs after
        // the fresh-file edge loop during an incremental reindex) was
        // stamped `receiver_type=''` and then permanently skipped by
        // `resolve_null_target_edges`, since that repair pass had no
        // import context of its own to retry with. Persisting the
        // candidate list lets the repair pass retry the same exact-match
        // import tier once the target's symbol row exists.
        if !has_column(conn, "edges", "import_candidates")? {
            conn.execute("ALTER TABLE edges ADD COLUMN import_candidates TEXT", [])?;
        }
    }

    if existing < 15 {
        // At most DEFAULT_GRAPH_VERSION_RETENTION (3) versions are kept, so
        // a graph_version index matches a third of the rows or more -- but
        // without planner stats SQLite rates
        // it as selective as idx_edges_target and picked it for every
        // per-symbol edge lookup, making each one a full scan (dpb:
        // dead_symbols 384s -> 0.19s, trace_flow 5s -> 0.09s once dropped).
        // ANALYZE fixes the plan too, but pooled read connections keep
        // stale stats until reopened; dropping the index needs no upkeep.
        conn.execute_batch(
            "DROP INDEX IF EXISTS idx_symbols_graph_version;
             DROP INDEX IF EXISTS idx_edges_graph_version;",
        )?;
    }

    if existing < 16 {
        // Issue #75: guarded name-fallback visibility rules. `visibility`
        // is written only by extractors that record an explicit modifier
        // (Rust `pub`, C#/TS `private`) -- NULL means "not recorded",
        // which the resolver treats as unrestricted, same as before this
        // column existed. `bare_call` mirrors `EdgeInput::bare_call`: true
        // for a genuinely bare identifier call (`foo()`), false for any
        // receiver-qualified one (`obj.foo()`, `self.foo()`,
        // `Type::method()`) or for an edge kind that doesn't set it —
        // existing rows default to `0` (not confirmed bare) so nothing
        // pre-migration is newly restricted until it's re-resolved.
        if !has_column(conn, "symbols", "visibility")? {
            conn.execute("ALTER TABLE symbols ADD COLUMN visibility TEXT", [])?;
        }
        if !has_column(conn, "edges", "bare_call")? {
            conn.execute(
                "ALTER TABLE edges ADD COLUMN bare_call INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
    }

    if existing < 17 {
        // Issue #76: symbol ids were a plain `INTEGER PRIMARY KEY` (SQLite
        // reuses a freed rowid for the next insert) with no foreign key on
        // `edges.source_symbol_id`/`target_symbol_id`. An incremental
        // rename in watch mode could free a symbol's rowid and hand it to
        // an unrelated symbol, silently making a stale edge reference
        // point at the wrong target instead of going dangling. Neither
        // AUTOINCREMENT nor a foreign key can be added with ALTER TABLE,
        // so both tables are rebuilt in place: create the new shape, copy
        // rows across (nulling any target that's already dangling), drop
        // the old table, rename the new one in. Existing rows keep their
        // ids -- only future inserts get the never-reused guarantee.
        eprintln!(
            "lidx: migrating to schema v17 -- rebuilding symbols (never-reused id sequence) \
             and edges (on-delete-set-null foreign key on source/target symbol ids) tables"
        );
        migrate_symbol_id_sequence(conn)?;
    }

    if existing < 18 {
        // Issue #78: an unresolved reference becomes a first-class,
        // retriable row instead of just a NULL `edges.target_symbol_id`.
        // `Db::insert_edges` writes one row here for every
        // `Resolver::resolve` call that returns `Unresolved` (skipped when
        // there's no reference name or import candidate to key on at all
        // -- a targetless edge has nothing for a retry to match against).
        // `name_tail` is the reference's trailing name segment
        // (`resolver::qualname_trailing_name`), what
        // `Db::retry_unresolved_references` joins against newly inserted
        // symbols to find references worth retrying, instead of
        // rescanning every NULL-target edge. `edge_id` is `UNIQUE`: an
        // edge has at most one current unresolved outcome.
        //
        // NULL-target edges are still written and still repaired by
        // `resolve_null_target_edges` unchanged -- this table is
        // additive, not yet the read path's source of truth (that
        // contract change is a follow-up).
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS unresolved_references (
                id INTEGER PRIMARY KEY,
                edge_id INTEGER NOT NULL UNIQUE,
                source_symbol_id INTEGER,
                file_id INTEGER NOT NULL,
                edge_kind TEXT NOT NULL,
                reference_name TEXT,
                name_tail TEXT NOT NULL,
                reason TEXT NOT NULL,
                import_candidates TEXT,
                graph_version INTEGER NOT NULL,
                FOREIGN KEY(edge_id) REFERENCES edges(id) ON DELETE CASCADE,
                FOREIGN KEY(source_symbol_id) REFERENCES symbols(id) ON DELETE SET NULL,
                FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_unresolved_references_name
                ON unresolved_references(reference_name);
            CREATE INDEX IF NOT EXISTS idx_unresolved_references_name_tail
                ON unresolved_references(name_tail);
            CREATE INDEX IF NOT EXISTS idx_unresolved_references_gv
                ON unresolved_references(graph_version);
            CREATE INDEX IF NOT EXISTS idx_unresolved_references_reason
                ON unresolved_references(reason);",
        )?;
    }

    if existing < SCHEMA_VERSION {
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('schema_version', ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [SCHEMA_VERSION.to_string()],
        )?;
    }

    Ok(())
}

fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    for row in rows {
        if row? == column {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Rebuilds `symbols` (id -> `INTEGER PRIMARY KEY AUTOINCREMENT`) and
/// `edges` (adds `ON DELETE SET NULL` foreign keys on `source_symbol_id`
/// and `target_symbol_id`) in place, for schema v17 (issue #76).
///
/// SQLite can't `ALTER TABLE` a column to add `AUTOINCREMENT` or a foreign
/// key, so this is the standard SQLite table-rebuild recipe: create the new
/// table, copy rows across, drop the old one, rename the new one in. Both
/// tables are rebuilt in the same pass because the new `edges` foreign keys
/// target `symbols(id)`, which must already have its final shape.
///
/// Column lists are the full v16 shape (every column added by every
/// migration through `existing < 16` above), not just the base schema at
/// the top of this file -- by the time this runs, every earlier
/// version-gated block has already applied to this connection, whether the
/// database is old or brand new (see this function's caller).
///
/// The `edges` copy's `src`/`tgt` joins also require `graph_version` to
/// match the edge's own, not just the id: a pre-v17 database can carry a
/// source/target id that exists in `symbols`, just under a different
/// graph version (the same legacy drift `repair_dangling_symbol_ids` used
/// to clean up at runtime, now dead code since nothing produces it going
/// forward -- see issue #76's review). Matching on id alone would let that
/// stale reference survive the migration as a live, wrong-but-non-NULL
/// target instead of being nulled here, once and for all, for any
/// database that still has one.
fn migrate_symbol_id_sequence(conn: &Connection) -> Result<()> {
    // Foreign key enforcement can only be toggled outside of a transaction,
    // and must be off for the duration: with it on, SQLite refuses to drop
    // a table that another table's (unrelated, pre-existing) foreign key
    // still points at -- `symbol_metrics.symbol_id` already references
    // `symbols(id)` today.
    conn.execute_batch("PRAGMA foreign_keys = OFF;")?;

    conn.execute_batch(
        "
        BEGIN;

        CREATE TABLE symbols_v17 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER NOT NULL,
            kind TEXT NOT NULL,
            name TEXT NOT NULL,
            qualname TEXT NOT NULL,
            start_line INTEGER NOT NULL,
            start_col INTEGER NOT NULL,
            end_line INTEGER NOT NULL,
            end_col INTEGER NOT NULL,
            start_byte INTEGER NOT NULL,
            end_byte INTEGER NOT NULL,
            signature TEXT,
            docstring TEXT,
            graph_version INTEGER NOT NULL DEFAULT 1,
            commit_sha TEXT,
            stable_id TEXT,
            visibility TEXT,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        );

        INSERT INTO symbols_v17
            (id, file_id, kind, name, qualname, start_line, start_col, end_line, end_col,
             start_byte, end_byte, signature, docstring, graph_version, commit_sha,
             stable_id, visibility)
        SELECT id, file_id, kind, name, qualname, start_line, start_col, end_line, end_col,
               start_byte, end_byte, signature, docstring, graph_version, commit_sha,
               stable_id, visibility
        FROM symbols;

        DROP TABLE symbols;
        ALTER TABLE symbols_v17 RENAME TO symbols;

        CREATE INDEX IF NOT EXISTS idx_symbols_name ON symbols(name);
        CREATE INDEX IF NOT EXISTS idx_symbols_qualname ON symbols(qualname);
        CREATE INDEX IF NOT EXISTS idx_symbols_file ON symbols(file_id);
        CREATE INDEX IF NOT EXISTS idx_symbols_stable_id ON symbols(stable_id);
        CREATE INDEX IF NOT EXISTS idx_symbols_name_kind ON symbols(name, kind);

        CREATE TABLE edges_v17 (
            id INTEGER PRIMARY KEY,
            file_id INTEGER NOT NULL,
            source_symbol_id INTEGER,
            target_symbol_id INTEGER,
            kind TEXT NOT NULL,
            target_qualname TEXT,
            detail TEXT,
            evidence_snippet TEXT,
            evidence_start_line INTEGER,
            evidence_end_line INTEGER,
            confidence REAL,
            graph_version INTEGER NOT NULL DEFAULT 1,
            commit_sha TEXT,
            trace_id TEXT,
            span_id TEXT,
            event_ts INTEGER,
            receiver_type TEXT,
            resolution_kind TEXT,
            import_candidates TEXT,
            bare_call INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,
            FOREIGN KEY(source_symbol_id) REFERENCES symbols(id) ON DELETE SET NULL,
            FOREIGN KEY(target_symbol_id) REFERENCES symbols(id) ON DELETE SET NULL
        );

        INSERT INTO edges_v17
            (id, file_id, source_symbol_id, target_symbol_id, kind, target_qualname, detail,
             evidence_snippet, evidence_start_line, evidence_end_line, confidence,
             graph_version, commit_sha, trace_id, span_id, event_ts, receiver_type,
             resolution_kind, import_candidates, bare_call)
        SELECT e.id, e.file_id,
               CASE WHEN src.id IS NULL THEN NULL ELSE e.source_symbol_id END,
               CASE WHEN tgt.id IS NULL THEN NULL ELSE e.target_symbol_id END,
               e.kind, e.target_qualname, e.detail, e.evidence_snippet,
               e.evidence_start_line, e.evidence_end_line, e.confidence,
               e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
               e.receiver_type, e.resolution_kind, e.import_candidates, e.bare_call
        FROM edges e
        LEFT JOIN symbols src ON src.id = e.source_symbol_id AND src.graph_version = e.graph_version
        LEFT JOIN symbols tgt ON tgt.id = e.target_symbol_id AND tgt.graph_version = e.graph_version;

        DROP TABLE edges;
        ALTER TABLE edges_v17 RENAME TO edges;

        CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_symbol_id);
        CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_symbol_id);
        CREATE INDEX IF NOT EXISTS idx_edges_file ON edges(file_id);
        CREATE INDEX IF NOT EXISTS idx_edges_trace ON edges(trace_id);
        CREATE INDEX IF NOT EXISTS idx_edges_event_ts ON edges(event_ts);
        CREATE INDEX IF NOT EXISTS idx_edges_target_qualname ON edges(target_qualname);

        COMMIT;
        ",
    )?;

    conn.execute_batch("PRAGMA foreign_keys = ON;")?;

    // Belt and suspenders: the copy above already nulled any target that
    // didn't resolve to a live symbol row, so this should never find
    // anything -- but a silent foreign key violation surviving a schema
    // migration is exactly the bug class this issue exists to close.
    let violations: i64 =
        conn.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
            row.get(0)
        })?;
    if violations > 0 {
        bail!(
            "lidx: {violations} foreign key violation(s) remain after migrating to schema v17 \
             -- refusing to continue"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-builds the pre-v17 schema shape (plain `INTEGER PRIMARY KEY` on
    /// `symbols`, no foreign key on `edges.source_symbol_id`/
    /// `target_symbol_id`) that a real database created before this
    /// migration existed would have, stamped at schema_version 16 -- the
    /// version `migrate` sees just before the v17 block below runs.
    fn open_v16_db(conn: &Connection) {
        conn.execute_batch(
            "
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE files (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL UNIQUE,
                hash TEXT NOT NULL,
                language TEXT NOT NULL,
                size INTEGER NOT NULL,
                modified INTEGER NOT NULL,
                deleted_version INTEGER
            );
            CREATE TABLE symbols (
                id INTEGER PRIMARY KEY,
                file_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                name TEXT NOT NULL,
                qualname TEXT NOT NULL,
                start_line INTEGER NOT NULL,
                start_col INTEGER NOT NULL,
                end_line INTEGER NOT NULL,
                end_col INTEGER NOT NULL,
                start_byte INTEGER NOT NULL,
                end_byte INTEGER NOT NULL,
                signature TEXT,
                docstring TEXT,
                graph_version INTEGER NOT NULL DEFAULT 1,
                commit_sha TEXT,
                stable_id TEXT,
                visibility TEXT,
                FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
            );
            CREATE TABLE edges (
                id INTEGER PRIMARY KEY,
                file_id INTEGER NOT NULL,
                source_symbol_id INTEGER,
                target_symbol_id INTEGER,
                kind TEXT NOT NULL,
                target_qualname TEXT,
                detail TEXT,
                evidence_snippet TEXT,
                evidence_start_line INTEGER,
                evidence_end_line INTEGER,
                confidence REAL,
                graph_version INTEGER NOT NULL DEFAULT 1,
                commit_sha TEXT,
                trace_id TEXT,
                span_id TEXT,
                event_ts INTEGER,
                receiver_type TEXT,
                resolution_kind TEXT,
                import_candidates TEXT,
                bare_call INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
            );
            CREATE TABLE symbol_metrics (
                id INTEGER PRIMARY KEY,
                symbol_id INTEGER NOT NULL UNIQUE,
                file_id INTEGER NOT NULL,
                loc INTEGER NOT NULL,
                complexity INTEGER NOT NULL,
                duplication_hash TEXT,
                FOREIGN KEY(symbol_id) REFERENCES symbols(id) ON DELETE CASCADE,
                FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
            );
            INSERT INTO meta (key, value) VALUES ('schema_version', '16');
            ",
        )
        .unwrap();
    }

    #[test]
    fn migrates_existing_v16_db_to_never_reused_ids_and_nulls_dangling_targets() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        open_v16_db(&conn);

        conn.execute(
            "INSERT INTO files (id, path, hash, language, size, modified) \
             VALUES (1, 'a.py', 'h', 'python', 1, 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols \
                (id, file_id, kind, name, qualname, start_line, start_col, end_line, end_col, \
                 start_byte, end_byte, graph_version) \
             VALUES (5, 1, 'function', 'foo', 'a.foo', 1, 0, 1, 0, 0, 0, 1)",
            [],
        )
        .unwrap();
        // A pre-existing dangling target: nothing has id 999. Exercises
        // "the migration handles existing dangling targets" directly.
        conn.execute(
            "INSERT INTO edges (id, file_id, source_symbol_id, target_symbol_id, kind, graph_version) \
             VALUES (1, 1, 5, 999, 'CALLS', 1)",
            [],
        )
        .unwrap();

        migrate(&conn).unwrap();

        let version: String = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION.to_string());

        let symbols_sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'symbols'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            symbols_sql.contains("AUTOINCREMENT"),
            "migrated symbols table should be AUTOINCREMENT: {symbols_sql}"
        );

        // Pre-existing dangling target must be nulled, not dropped or left
        // pointing at the nonexistent id.
        let target: Option<i64> = conn
            .query_row(
                "SELECT target_symbol_id FROM edges WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(target, None);

        let violations: i64 = conn
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(violations, 0);

        // A second edge, added post-migration, that targets symbol 5 --
        // covers the `target_symbol_id` foreign key specifically, since
        // edge 1's target (999) was already NULL before this delete (it
        // never existed, so the migration's copy nulled it on the way in).
        conn.execute(
            "INSERT INTO edges (id, file_id, source_symbol_id, target_symbol_id, kind, graph_version) \
             VALUES (2, 1, NULL, 5, 'CALLS', 1)",
            [],
        )
        .unwrap();

        // The foreign key now really enforces on-delete-set-null: deleting
        // the referenced symbol nulls both edges instead of leaving either
        // dangling.
        conn.execute("DELETE FROM symbols WHERE id = 5", [])
            .unwrap();
        let source: Option<i64> = conn
            .query_row(
                "SELECT source_symbol_id FROM edges WHERE id = 1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(source, None);
        let target: Option<i64> = conn
            .query_row(
                "SELECT target_symbol_id FROM edges WHERE id = 2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(target, None);

        // A fresh insert never reuses the now-free id 5.
        conn.execute(
            "INSERT INTO symbols \
                (file_id, kind, name, qualname, start_line, start_col, end_line, end_col, \
                 start_byte, end_byte, graph_version) \
             VALUES (1, 'function', 'bar', 'a.bar', 2, 0, 2, 0, 0, 0, 1)",
            [],
        )
        .unwrap();
        let new_id: i64 = conn
            .query_row(
                "SELECT id FROM symbols WHERE qualname = 'a.bar'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            new_id > 5,
            "new symbol id {new_id} must not reuse the freed id 5"
        );
    }

    #[test]
    fn migrate_is_idempotent_on_an_already_migrated_db() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        migrate(&conn).unwrap();
        // Running migrate again on an up-to-date database must be a no-op,
        // not fail or re-run the v17 rebuild.
        migrate(&conn).unwrap();

        let version: String = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION.to_string());
    }

    #[test]
    fn migrate_creates_unresolved_references_table_with_cascading_fks() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        migrate(&conn).unwrap();

        for column in [
            "id",
            "edge_id",
            "source_symbol_id",
            "file_id",
            "edge_kind",
            "reference_name",
            "name_tail",
            "reason",
            "import_candidates",
            "graph_version",
        ] {
            assert!(
                has_column(&conn, "unresolved_references", column).unwrap(),
                "unresolved_references missing column {column}"
            );
        }

        conn.execute(
            "INSERT INTO files (id, path, hash, language, size, modified) \
             VALUES (1, 'a.py', 'h', 'python', 1, 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO symbols \
                (id, file_id, kind, name, qualname, start_line, start_col, end_line, end_col, \
                 start_byte, end_byte, graph_version) \
             VALUES (1, 1, 'function', 'foo', 'a.foo', 1, 0, 1, 0, 0, 0, 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO edges (id, file_id, source_symbol_id, kind, target_qualname, graph_version) \
             VALUES (1, 1, 1, 'CALLS', 'bar', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO unresolved_references \
                (edge_id, source_symbol_id, file_id, edge_kind, reference_name, name_tail, reason, graph_version) \
             VALUES (1, 1, 1, 'CALLS', 'bar', 'bar', 'no_candidates', 1)",
            [],
        )
        .unwrap();

        // Deleting the edge (a reindex deleting and re-inserting a file's
        // edges, or a pruned graph version) must cascade -- otherwise a
        // stale row accumulates forever, per issue #78's cleanup requirement.
        conn.execute("DELETE FROM edges WHERE id = 1", []).unwrap();
        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM unresolved_references", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            remaining, 0,
            "deleting the edge must cascade to its unresolved_references row"
        );
    }
}
