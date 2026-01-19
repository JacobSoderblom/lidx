use crate::diagnostics::DiagnosticInput;
use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};

pub const SCHEMA_VERSION: i64 = 11;

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

        CREATE TABLE IF NOT EXISTS diagnostics (
            id INTEGER PRIMARY KEY,
            file_id INTEGER,
            path TEXT,
            line INTEGER,
            column INTEGER,
            end_line INTEGER,
            end_column INTEGER,
            severity TEXT,
            message TEXT NOT NULL,
            rule_id TEXT,
            tool TEXT,
            snippet TEXT,
            diagnostic_hash TEXT,
            created INTEGER NOT NULL,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_diagnostics_file ON diagnostics(file_id);
        CREATE INDEX IF NOT EXISTS idx_diagnostics_path ON diagnostics(path);
        CREATE INDEX IF NOT EXISTS idx_diagnostics_severity ON diagnostics(severity);
        CREATE INDEX IF NOT EXISTS idx_diagnostics_rule ON diagnostics(rule_id);
        CREATE INDEX IF NOT EXISTS idx_diagnostics_tool ON diagnostics(tool);
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

    if existing < 3 {
        if !has_column(conn, "edges", "evidence_snippet")? {
            conn.execute("ALTER TABLE edges ADD COLUMN evidence_snippet TEXT", [])?;
        }
    }

    if existing < 5 {
        if !has_column(conn, "diagnostics", "diagnostic_hash")? {
            conn.execute(
                "ALTER TABLE diagnostics ADD COLUMN diagnostic_hash TEXT",
                [],
            )?;
        }
        backfill_diagnostics_hashes(conn)?;
        conn.execute(
            "DELETE FROM diagnostics
             WHERE id NOT IN (
                SELECT MIN(id) FROM diagnostics GROUP BY diagnostic_hash
             )",
            [],
        )?;
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_diagnostics_hash ON diagnostics(diagnostic_hash)",
            [],
        )?;
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
            "CREATE INDEX IF NOT EXISTS idx_symbols_graph_version ON symbols(graph_version)",
            [],
        )?;
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_edges_graph_version ON edges(graph_version)",
            [],
        )?;
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

fn backfill_diagnostics_hashes(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT id, path, line, column, end_line, end_column, severity, message, rule_id, tool, snippet
         FROM diagnostics",
    )?;
    let rows = stmt.query_map([], |row| {
        let id: i64 = row.get(0)?;
        let diagnostic = DiagnosticInput {
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
        };
        Ok((id, diagnostic))
    })?;
    let mut update = conn.prepare("UPDATE diagnostics SET diagnostic_hash = ? WHERE id = ?")?;
    for row in rows {
        let (id, diagnostic) = row?;
        let hash = diagnostic.fingerprint();
        update.execute(params![hash, id])?;
    }
    Ok(())
}
