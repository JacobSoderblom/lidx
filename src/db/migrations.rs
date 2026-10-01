use anyhow::{Result, bail};
use rusqlite::{Connection, OptionalExtension, params};

pub const SCHEMA_VERSION: i64 = 27;

/// The identity columns of an `unresolved_references` row beyond
/// `(graph_version, file_id)`, NULL-normalised so the unique index treats
/// NULLs as equal (issue #251).
const UNRESOLVED_IDENTITY_EXPRS: &str = "COALESCE(source_symbol_id, -1), edge_kind, \
COALESCE(reference_name, ''), COALESCE(evidence_start_line, -1), COALESCE(evidence_end_line, -1)";

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
        // carried forward yet (see `carry_forward_references`, which runs after
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

    if existing < 19 {
        // Issue #79: the write path stops writing a NULL-target edge for
        // every kind except a Bridge Edge (`indexer::channel::
        // is_bridge_edge_kind` -- its target is a cross-language/
        // cross-process join key, not necessarily a symbol in this graph,
        // and `trace_flow`'s bridging reads it straight off `edges`
        // regardless of resolution, so it keeps its placeholder edge
        // exactly as before). Every other kind's unresolved reference now
        // lives only in `unresolved_references`, so that table can no
        // longer key on a live edge (`edge_id` becomes nullable) and must
        // carry everything a retry needs to either update that edge
        // (Bridge Edge kinds) or insert a brand new one from scratch
        // (everything else) -- see `db::resolver`'s module doc.
        eprintln!(
            "lidx: migrating to schema v19 -- moving existing non-Bridge-Edge-kind \
             NULL-target edges into the unresolved-reference store and deleting them"
        );
        migrate_unresolved_reference_store_v19(conn)?;
    }

    if existing < 20 {
        // Issue #80: the resolver's known-external outcome (an import
        // known to resolve outside the repo, or a language-specific
        // known-external fallback -- see `db::resolver`'s module doc)
        // binds to a stub symbol (`kind = 'external'`, qualname `ext:...`)
        // instead of leaving the reference unresolved. One stub per
        // `(graph_version, qualname)`, reused across every call site that
        // shares it (`Resolver::resolve_external_stub`) and copied forward
        // wholesale on every reindex (`Db::carry_forward_references`) so a
        // carried-forward edge's stable_id-based remap always finds its
        // target. This partial unique index is what makes both of those
        // idempotent: an `INSERT ... ON CONFLICT DO NOTHING` against the
        // same `(graph_version, qualname)` pair is a no-op instead of a
        // duplicate stub.
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_symbols_external_stub
             ON symbols(graph_version, qualname) WHERE kind = 'external'",
            [],
        )?;
    }

    if existing < 21 {
        // Issues #123/#124: `call_shape` mirrors `EdgeInput::call_shape`
        // (`"<n>"` / `"new:<n>"`, NULL = no arity signal), on the edge and
        // on the unresolved-reference store's shadow row so a retry
        // re-judges an overloaded call by the same arity.
        for table in ["edges", "unresolved_references"] {
            if !has_column(conn, table, "call_shape")? {
                conn.execute(
                    &format!("ALTER TABLE {table} ADD COLUMN call_shape TEXT"),
                    [],
                )?;
            }
        }
    }

    if existing < 22 {
        // Issue #173: interface dispatch walks IMPLEMENTS/EXTENDS edges
        // (recursive CTE in `graph_query::dispatch_pairs_from`); this keeps
        // that walk off a full `edges` scan.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_edges_kind_source ON edges(kind, source_symbol_id)",
            [],
        )?;
    }

    if existing < 23 {
        // Deferred-receiver call sites (`receiver_type` = `@ret:...`) and the
        // `RPC_CALL` edges derived from them are rescanned on every repair
        // pass (`Db::rederive_deferred_rpc_calls`); these keep those scans
        // off a full table walk.
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_edges_deferred_sites
                ON edges(graph_version)
                WHERE kind = 'CALLS' AND receiver_type LIKE '@ret:%';
             CREATE INDEX IF NOT EXISTS idx_edges_derived_rpc
                ON edges(graph_version) WHERE call_shape = 'rpc:deferred';
             CREATE INDEX IF NOT EXISTS idx_unresolved_deferred_sites
                ON unresolved_references(graph_version)
                WHERE edge_kind = 'CALLS' AND receiver_type LIKE '@ret:%';",
        )?;
    }

    if existing < 24 {
        // Typed deferred-resolution storage. `receiver_type` used to carry
        // `@ret:` / `@ret:r:` / `@arg:` markers and the C# type scope
        // (`enclosing;usings|Type`), and a derived RPC edge was tagged
        // `call_shape = 'rpc:deferred'`. Each now has its own column; existing
        // rows are converted in place.
        for table in ["edges", "unresolved_references"] {
            for column in ["receiver_scope", "deferred_kind", "deferred"] {
                if !has_column(conn, table, column)? {
                    conn.execute(&format!("ALTER TABLE {table} ADD COLUMN {column} TEXT"), [])?;
                }
            }
        }
        if !has_column(conn, "edges", "derived")? {
            conn.execute(
                "ALTER TABLE edges ADD COLUMN derived INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        conn.execute(
            "UPDATE edges SET derived = 1, call_shape = NULL WHERE call_shape = 'rpc:deferred'",
            [],
        )?;
        let tx = conn.unchecked_transaction()?;
        let mut dropped = 0;
        for table in ["edges", "unresolved_references"] {
            dropped += legacy_receiver::convert_table(&tx, table)?;
        }
        tx.commit()?;
        if dropped > 0 {
            eprintln!(
                "migration 24: {dropped} deferred receiver marker(s) could not be parsed; \
                 their receivers are now untracked"
            );
        }
        conn.execute_batch(
            "DROP INDEX IF EXISTS idx_edges_deferred_sites;
             DROP INDEX IF EXISTS idx_edges_derived_rpc;
             DROP INDEX IF EXISTS idx_unresolved_deferred_sites;
             CREATE INDEX IF NOT EXISTS idx_edges_deferred_sites
                ON edges(graph_version, kind) WHERE deferred_kind IS NOT NULL;
             CREATE INDEX IF NOT EXISTS idx_edges_derived_rpc
                ON edges(graph_version) WHERE derived = 1;
             CREATE INDEX IF NOT EXISTS idx_unresolved_deferred_sites
                ON unresolved_references(graph_version, edge_kind)
                WHERE deferred_kind IS NOT NULL;",
        )?;
    }

    if existing < 25 {
        // Issue #254: each FK on `unresolved_references` needs an index on
        // its referencing column, or every parent delete (a graph-version
        // prune deletes ~18k symbols) full-scans the table. `edge_id` is
        // covered by its UNIQUE autoindex and `edges` already indexes all
        // three of its FK columns; these two were the gaps.
        eprintln!("migration 25: indexing unresolved_references FK columns");
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_unresolved_references_source
                ON unresolved_references(source_symbol_id);
             CREATE INDEX IF NOT EXISTS idx_unresolved_references_file
                ON unresolved_references(file_id);",
        )?;
    }

    if existing < 26 {
        // Issue #250: a graph version is `building` until a reindex has fully
        // populated it, and only then `complete` (and current). Every
        // pre-existing row was made current at creation, so it is complete.
        if !has_column(conn, "graph_versions", "status")? {
            conn.execute(
                "ALTER TABLE graph_versions ADD COLUMN status TEXT NOT NULL DEFAULT 'complete'",
                [],
            )?;
        }
    }

    if existing < 27 {
        // Issue #251: a reference's identity within a graph version is its
        // file, source symbol, kind, name and evidence span. Before this,
        // nothing enforced it, so carry-forward plus the cross-language link
        // pass each contributed a row for the same pending ROUTE reference
        // on every reindex. Collapse existing duplicates (keeping the row
        // bound to a Bridge Edge if any, else the oldest), then make the
        // invariant structural with a unique index.
        eprintln!("migration 27: collapsing duplicate unresolved_references rows");
        conn.execute_batch(&format!(
            "DELETE FROM unresolved_references WHERE id IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY graph_version, file_id, {UNRESOLVED_IDENTITY_EXPRS}
                        ORDER BY (edge_id IS NULL), id) AS rn
                    FROM unresolved_references)
                WHERE rn > 1);
             CREATE UNIQUE INDEX IF NOT EXISTS idx_unresolved_references_identity
                ON unresolved_references(graph_version, file_id, {UNRESOLVED_IDENTITY_EXPRS});"
        ))?;
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

/// One pre-migration `unresolved_references` (v18) row, joined to its edge
/// for the shadow columns v19 adds -- see `migrate_unresolved_reference_store_v19`.
struct LegacyUnresolvedRow {
    edge_id: i64,
    source_symbol_id: Option<i64>,
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
    graph_version: i64,
}

/// One pre-migration NULL-target edge with no `unresolved_references` (v18)
/// row at all yet -- the same gap `Db::reconcile_unresolved_reference_store`
/// closes at runtime, just not yet reached by a repair pass on this
/// database. See `migrate_unresolved_reference_store_v19`.
struct GapEdgeRow {
    edge_id: i64,
    source_symbol_id: Option<i64>,
    file_id: i64,
    edge_kind: String,
    target_qualname: Option<String>,
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
    graph_version: i64,
}

/// Schema v19 (issue #79): rebuilds `unresolved_references` self-contained
/// (nullable `edge_id`, plus the shadow columns a retry needs to rebuild an
/// edge from scratch -- receiver_type, bare_call, evidence lines/snippet,
/// detail, confidence, commit_sha, trace/span id, event_ts) and moves every
/// existing non-Bridge-Edge-kind NULL-target edge into it, deleting the
/// edge. A Bridge Edge kind's row keeps its `edge_id`, since that edge stays
/// (unchanged from before this migration).
///
/// Two shapes of pre-existing row, mirroring
/// `Db::reconcile_unresolved_reference_store`'s own two cases:
/// - a v18 `unresolved_references` row already exists for the edge --
///   promoted in place, its shadow columns copied from the edge it names.
/// - no v18 row exists yet (the same gap a repair pass closes at runtime,
///   just not yet reached on this database) -- created fresh, with a
///   best-effort `reason` of `no_candidates`: this migration doesn't run the
///   resolver, and the next repair pass re-derives the real reason the same
///   way it always has. A row with neither a `target_qualname` nor an
///   import candidate has nothing for a retry to key on and is left
///   untouched, same as `reconcile_unresolved_reference_store` leaves it
///   today.
///
/// SQLite can't relax a `NOT NULL`/`UNIQUE` column with `ALTER TABLE`, so
/// this is the standard rebuild recipe (see `migrate_symbol_id_sequence`):
/// build the new table, populate it, drop the old one, rename the new one
/// in -- wrapped in one transaction since `conn` is a shared reference here
/// (no `Connection::transaction()` available), same as that function.
fn migrate_unresolved_reference_store_v19(conn: &Connection) -> Result<()> {
    conn.execute("BEGIN;", [])?;

    conn.execute_batch(
        "CREATE TABLE unresolved_references_v19 (
            id INTEGER PRIMARY KEY,
            edge_id INTEGER UNIQUE,
            source_symbol_id INTEGER,
            file_id INTEGER NOT NULL,
            edge_kind TEXT NOT NULL,
            reference_name TEXT,
            name_tail TEXT NOT NULL,
            reason TEXT NOT NULL,
            import_candidates TEXT,
            detail TEXT,
            evidence_snippet TEXT,
            evidence_start_line INTEGER,
            evidence_end_line INTEGER,
            confidence REAL,
            commit_sha TEXT,
            trace_id TEXT,
            span_id TEXT,
            event_ts INTEGER,
            receiver_type TEXT,
            bare_call INTEGER NOT NULL DEFAULT 0,
            graph_version INTEGER NOT NULL,
            FOREIGN KEY(edge_id) REFERENCES edges(id) ON DELETE CASCADE,
            FOREIGN KEY(source_symbol_id) REFERENCES symbols(id) ON DELETE SET NULL,
            FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE
        );",
    )?;

    // Read both row shapes in full before writing anything -- the deletes
    // below must not shrink either result set out from under the other.
    let promote_rows: Vec<LegacyUnresolvedRow> = {
        let mut stmt = conn.prepare(
            "SELECT ur.edge_id, ur.source_symbol_id, ur.file_id, ur.edge_kind,
                    ur.reference_name, ur.name_tail, ur.reason, ur.import_candidates,
                    e.detail, e.evidence_snippet, e.evidence_start_line, e.evidence_end_line,
                    e.confidence, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.receiver_type, e.bare_call, ur.graph_version
             FROM unresolved_references ur
             JOIN edges e ON e.id = ur.edge_id",
        )?;
        stmt.query_map([], |row| {
            Ok(LegacyUnresolvedRow {
                edge_id: row.get(0)?,
                source_symbol_id: row.get(1)?,
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
                graph_version: row.get(19)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    };

    let gap_rows: Vec<GapEdgeRow> = {
        let mut stmt = conn.prepare(
            "SELECT e.id, e.source_symbol_id, e.file_id, e.kind, e.target_qualname,
                    e.import_candidates, e.detail, e.evidence_snippet, e.evidence_start_line,
                    e.evidence_end_line, e.confidence, e.commit_sha, e.trace_id, e.span_id,
                    e.event_ts, e.receiver_type, e.bare_call, e.graph_version
             FROM edges e
             LEFT JOIN unresolved_references ur ON ur.edge_id = e.id
             WHERE e.target_symbol_id IS NULL
               AND ur.id IS NULL
               AND (e.target_qualname IS NOT NULL OR e.import_candidates IS NOT NULL)",
        )?;
        stmt.query_map([], |row| {
            Ok(GapEdgeRow {
                edge_id: row.get(0)?,
                source_symbol_id: row.get(1)?,
                file_id: row.get(2)?,
                edge_kind: row.get(3)?,
                target_qualname: row.get(4)?,
                import_candidates: row.get(5)?,
                detail: row.get(6)?,
                evidence_snippet: row.get(7)?,
                evidence_start_line: row.get(8)?,
                evidence_end_line: row.get(9)?,
                confidence: row.get(10)?,
                commit_sha: row.get(11)?,
                trace_id: row.get(12)?,
                span_id: row.get(13)?,
                event_ts: row.get(14)?,
                receiver_type: row.get(15)?,
                bare_call: row.get(16)?,
                graph_version: row.get(17)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
    };

    {
        let mut insert = conn.prepare(
            "INSERT INTO unresolved_references_v19
                (edge_id, source_symbol_id, file_id, edge_kind, reference_name, name_tail,
                 reason, import_candidates, detail, evidence_snippet, evidence_start_line,
                 evidence_end_line, confidence, commit_sha, trace_id, span_id, event_ts,
                 receiver_type, bare_call, graph_version)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )?;
        let mut delete_edge = conn.prepare("DELETE FROM edges WHERE id = ?")?;

        for row in &promote_rows {
            let is_bridge = crate::indexer::channel::is_bridge_edge_kind(&row.edge_kind);
            let edge_id = is_bridge.then_some(row.edge_id);
            insert.execute(params![
                edge_id,
                row.source_symbol_id,
                row.file_id,
                &row.edge_kind,
                &row.reference_name,
                &row.name_tail,
                &row.reason,
                &row.import_candidates,
                &row.detail,
                &row.evidence_snippet,
                row.evidence_start_line,
                row.evidence_end_line,
                row.confidence,
                &row.commit_sha,
                &row.trace_id,
                &row.span_id,
                row.event_ts,
                &row.receiver_type,
                row.bare_call,
                row.graph_version,
            ])?;
            if !is_bridge {
                delete_edge.execute(params![row.edge_id])?;
            }
        }

        for row in &gap_rows {
            let import_candidates: Vec<String> = row
                .import_candidates
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();
            let Some((reference_name, name_tail)) = super::resolver::store_reference_name_and_tail(
                row.target_qualname.as_deref(),
                &import_candidates,
            ) else {
                continue;
            };
            let is_bridge = crate::indexer::channel::is_bridge_edge_kind(&row.edge_kind);
            let edge_id = is_bridge.then_some(row.edge_id);
            insert.execute(params![
                edge_id,
                row.source_symbol_id,
                row.file_id,
                &row.edge_kind,
                reference_name,
                name_tail,
                "no_candidates",
                &row.import_candidates,
                &row.detail,
                &row.evidence_snippet,
                row.evidence_start_line,
                row.evidence_end_line,
                row.confidence,
                &row.commit_sha,
                &row.trace_id,
                &row.span_id,
                row.event_ts,
                &row.receiver_type,
                row.bare_call,
                row.graph_version,
            ])?;
            if !is_bridge {
                delete_edge.execute(params![row.edge_id])?;
            }
        }
    }

    conn.execute_batch(
        "DROP TABLE unresolved_references;
         ALTER TABLE unresolved_references_v19 RENAME TO unresolved_references;
         CREATE INDEX IF NOT EXISTS idx_unresolved_references_name
             ON unresolved_references(reference_name);
         CREATE INDEX IF NOT EXISTS idx_unresolved_references_name_tail
             ON unresolved_references(name_tail);
         CREATE INDEX IF NOT EXISTS idx_unresolved_references_gv
             ON unresolved_references(graph_version);
         CREATE INDEX IF NOT EXISTS idx_unresolved_references_reason
             ON unresolved_references(reason);
         COMMIT;",
    )?;

    Ok(())
}

/// Reads the pre-migration-24 string encodings of the receiver columns (the
/// `@ret:` / `@ret:r:` / `@arg:` markers and the `scope|Type` prefix) and
/// rewrites them into the typed columns. Frozen: this is the only code that
/// still understands those formats.
mod legacy_receiver {
    use crate::indexer::extract::{
        DeferredArgument, DeferredBase, DeferredMarker, DeferredReturn, DeferredSource,
        RustDeferred, Step,
    };
    use anyhow::Result;
    use rusqlite::{Connection, params};

    const RET: &str = "@ret:";
    const RUST: &str = "@ret:r:";
    const ARG: &str = "@arg:";

    /// Converts one table; returns how many markers failed to parse.
    pub(super) fn convert_table(conn: &Connection, table: &str) -> Result<usize> {
        let mut dropped = 0;
        let rows: Vec<(i64, String)> = {
            let mut stmt = conn.prepare(&format!(
                "SELECT id, receiver_type FROM {table}
                 WHERE receiver_type LIKE '@%'
                    OR (receiver_type LIKE '%|%'
                        AND file_id IN (SELECT id FROM files WHERE language = 'csharp'))"
            ))?;
            let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        let mut update = conn.prepare(&format!(
            "UPDATE {table} SET receiver_type = ?2, receiver_scope = ?3,
                    deferred_kind = ?4, deferred = ?5 WHERE id = ?1"
        ))?;
        for (id, column) in rows {
            if column.starts_with('@') {
                // A marker that no longer parses can't be re-judged; leave
                // the receiver untracked, as a fresh reindex would.
                let (kind, payload) = match parse_legacy_marker(&column).map(|m| m.encode()) {
                    Some((kind, payload)) => (Some(kind), Some(payload)),
                    None => {
                        dropped += 1;
                        (None, None)
                    }
                };
                update.execute(params![id, None::<String>, None::<String>, kind, payload])?;
            } else if let Some((scope, ty)) = column.split_once('|') {
                update.execute(params![id, ty, Some(scope), None::<String>, None::<String>])?;
            }
        }
        Ok(dropped)
    }

    /// The payload JSON comes from `DeferredMarker::encode`; a test in
    /// `extract.rs` pins its exact shape, so changing it breaks loudly and
    /// needs a new migration.
    fn parse_legacy_marker(column: &str) -> Option<DeferredMarker> {
        if let Some(rest) = column.strip_prefix(ARG) {
            let mut parts = rest.splitn(4, ':');
            let index = parts.next()?.parse().ok()?;
            let name = parts.next().filter(|n| !n.is_empty()).map(str::to_string);
            let arg_count = parts.next()?.parse().ok()?;
            return Some(DeferredMarker::Argument(DeferredArgument {
                index,
                name,
                arg_count,
                callee: parts.next()?.to_string(),
            }));
        }
        // `@ret:r:` (Rust) is a prefix-extension of `@ret:` (C#): test it first.
        if column.starts_with(RUST) {
            return parse_legacy_rust(column).map(DeferredMarker::Rust);
        }
        parse_legacy_ret(column).map(DeferredMarker::Return)
    }

    fn parse_legacy_ret(column: &str) -> Option<DeferredReturn> {
        let rest = column.strip_prefix(RET)?;
        let (flags, callee) = rest.split_once(':')?;
        if !flags.chars().all(|c| matches!(c, 'a' | 's' | 'n')) {
            return None;
        }
        let (base, method) = callee.rsplit_once('.')?;
        let base = if base.starts_with(RET) {
            DeferredBase::Call(Box::new(parse_legacy_ret(base)?))
        } else {
            DeferredBase::Type(base.to_string())
        };
        Some(DeferredReturn {
            base,
            method: method.to_string(),
            awaited: flags.contains('a'),
            static_only: flags.contains('s'),
            name_only: flags.contains('n'),
        })
    }

    fn parse_legacy_step(text: &str) -> Option<Step> {
        Some(match text {
            "some" => Step::OptionSome,
            "ok" => Step::ResultOk,
            "err" => Step::ResultErr,
            "elem" => Step::Elem,
            "await" => Step::Await,
            t if t.starts_with('t') => Step::Tuple(t[1..].parse().ok()?),
            t => Step::Method(t.strip_prefix('.')?.to_string()),
        })
    }

    fn parse_legacy_rust(column: &str) -> Option<RustDeferred> {
        let rest = column.strip_prefix(RUST)?;
        let mut parts = rest.splitn(5, '|');
        let (tag, x, y) = (parts.next()?, parts.next()?, parts.next()?);
        let source = match tag {
            "call" => DeferredSource::Call {
                candidates: x.split(';').map(str::to_string).collect(),
            },
            "method" => DeferredSource::Method {
                receiver_type: x.to_string(),
                method: y.to_string(),
            },
            "field" => DeferredSource::Field {
                owner: x.to_string(),
                field: y.to_string(),
            },
            _ => return None,
        };
        let steps = parts
            .next()?
            .split(',')
            .filter(|s| !s.is_empty())
            .map(parse_legacy_step)
            .collect::<Option<Vec<_>>>()?;
        let fallback = parts.next()?;
        Some(RustDeferred {
            source,
            steps,
            fallback: (!fallback.is_empty()).then(|| fallback.to_string()),
        })
    }
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
    fn migration_24_moves_string_encoded_receivers_into_typed_columns() {
        use crate::indexer::extract::{DeferredBase, DeferredMarker, DeferredReturn, TypeScope};

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        migrate(&conn).unwrap();
        conn.execute(
            "INSERT INTO files (id, path, hash, language, size, modified)
             VALUES (1, 'a.cs', 'h', 'csharp', 1, 1)",
            [],
        )
        .unwrap();
        let legacy = [
            (1, "@ret:as:Repo.Create", Some("1")),
            (2, "@arg:1:x:2:Helper.Make", Some("2")),
            (3, "A.B;N1,N2|IStore<int>", Some("3")),
            (4, "Plain", None),
            (5, "", None),
        ];
        for (id, receiver, shape) in legacy {
            conn.execute(
                "INSERT INTO edges (id, file_id, kind, target_qualname, graph_version,
                                    receiver_type, call_shape)
                 VALUES (?, 1, 'CALLS', 'x', 1, ?, ?)",
                params![id, receiver, shape],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO edges (id, file_id, kind, target_qualname, graph_version, call_shape)
             VALUES (6, 1, 'RPC_CALL', 'y', 1, 'rpc:deferred')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO files (id, path, hash, language, size, modified)
             VALUES (2, 'a.py', 'h', 'python', 1, 1)",
            [],
        )
        .unwrap();
        // Nested `@ret:` base, Rust marker, unparseable marker, a `|` in a
        // non-C# row, and unresolved_references rows.
        for (id, file, receiver) in [
            (7, 1, "@ret:s:@ret:a:Repo.Create.Load"),
            (8, 1, "@ret:r:method|Engine|build|some,.unwrap|Fallback"),
            (9, 1, "@ret:zz:garbage"),
            (10, 2, "py|keeps"),
        ] {
            conn.execute(
                "INSERT INTO edges (id, file_id, kind, target_qualname, graph_version, receiver_type)
                 VALUES (?, ?, 'CALLS', 'x', 1, ?)",
                params![id, file, receiver],
            )
            .unwrap();
        }
        for (id, receiver) in [(1, "@ret:as:Repo.Create"), (2, "N1|IStore")] {
            conn.execute(
                "INSERT INTO unresolved_references
                    (id, file_id, edge_kind, reference_name, name_tail, reason, graph_version,
                     receiver_type, evidence_start_line)
                 VALUES (?, 1, 'CALLS', 'x', 'x', 'no_candidates', 1, ?, ?)",
                params![id, receiver, id],
            )
            .unwrap();
        }
        conn.execute(
            "UPDATE meta SET value = '23' WHERE key = 'schema_version'",
            [],
        )
        .unwrap();
        migrate(&conn).unwrap();
        migrate(&conn).unwrap(); // idempotent

        type Row = (
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let row = |id: i64| -> Row {
            conn.query_row(
                "SELECT receiver_type, receiver_scope, deferred_kind, deferred
                 FROM edges WHERE id = ?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap()
        };
        let (ty, scope, kind, payload) = row(1);
        assert_eq!((ty, scope), (None, None));
        assert_eq!(
            DeferredMarker::decode(&kind.unwrap(), &payload.unwrap()),
            Some(DeferredMarker::Return(DeferredReturn::on_type(
                "Repo", "Create", true, true
            )))
        );
        let (ty, _, kind, payload) = row(2);
        assert_eq!(ty, None);
        assert!(matches!(
            DeferredMarker::decode(&kind.unwrap(), &payload.unwrap()),
            Some(DeferredMarker::Argument(a)) if a.callee == "Helper.Make" && a.arg_count == 2
        ));
        let (ty, scope, kind, _) = row(3);
        assert_eq!((ty.as_deref(), kind), (Some("IStore<int>"), None));
        let scope = TypeScope::decode(scope.as_deref());
        assert_eq!(
            (scope.enclosing, scope.usings),
            (
                vec!["A.B".to_string()],
                vec!["N1".to_string(), "N2".to_string()]
            )
        );
        assert_eq!(row(4).0.as_deref(), Some("Plain"));
        assert_eq!(row(5).0.as_deref(), Some(""));
        let (ty, _, kind, payload) = row(7);
        assert_eq!(ty, None);
        let Some(DeferredMarker::Return(outer)) =
            DeferredMarker::decode(&kind.unwrap(), &payload.unwrap())
        else {
            panic!("nested marker must convert");
        };
        assert!(outer.static_only && outer.method == "Load");
        let DeferredBase::Call(inner) = outer.base else {
            panic!("nested base must stay a call");
        };
        assert!(inner.awaited && inner.method == "Create");
        let (_, _, kind, payload) = row(8);
        assert!(matches!(
            DeferredMarker::decode(&kind.unwrap(), &payload.unwrap()),
            Some(DeferredMarker::Rust(r)) if r.fallback.as_deref() == Some("Fallback")
                && r.steps.len() == 2
        ));
        assert_eq!(row(9), (None, None, None, None));
        assert_eq!(row(10).0.as_deref(), Some("py|keeps"));
        let ur = |id: i64| -> Row {
            conn.query_row(
                "SELECT receiver_type, receiver_scope, deferred_kind, deferred
                 FROM unresolved_references WHERE id = ?",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap()
        };
        let (ty, scope, kind, payload) = ur(1);
        assert_eq!((ty, scope), (None, None));
        assert!(matches!(
            DeferredMarker::decode(&kind.unwrap(), &payload.unwrap()),
            Some(DeferredMarker::Return(_))
        ));
        let (ty, scope, kind, _) = ur(2);
        assert_eq!(
            (ty.as_deref(), scope.as_deref(), kind),
            (Some("IStore"), Some("N1"), None)
        );
        let (derived, shape): (i64, Option<String>) = conn
            .query_row(
                "SELECT derived, call_shape FROM edges WHERE id = 6",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((derived, shape), (1, None));
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

    /// Hand-builds the v18 schema shape (`unresolved_references.edge_id`
    /// `NOT NULL UNIQUE`, no shadow columns) a real database created before
    /// this migration existed would have, stamped at schema_version 18 --
    /// the version `migrate` sees just before the v19 block below runs.
    fn open_v18_db(conn: &Connection) {
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
                FOREIGN KEY(file_id) REFERENCES files(id) ON DELETE CASCADE,
                FOREIGN KEY(source_symbol_id) REFERENCES symbols(id) ON DELETE SET NULL,
                FOREIGN KEY(target_symbol_id) REFERENCES symbols(id) ON DELETE SET NULL
            );
            CREATE TABLE unresolved_references (
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
            INSERT INTO meta (key, value) VALUES ('schema_version', '18');
            ",
        )
        .unwrap();
    }

    #[test]
    fn migrates_existing_v18_db_to_self_contained_unresolved_references() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        open_v18_db(&conn);

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
             VALUES (1, 1, 'function', 'caller', 'a.caller', 1, 0, 1, 0, 0, 0, 1)",
            [],
        )
        .unwrap();

        // A plain CALLS edge, already NULL-target, with a v18 store row --
        // must be promoted (edge_id -> NULL) and its placeholder edge
        // deleted.
        conn.execute(
            "INSERT INTO edges \
                (id, file_id, source_symbol_id, kind, target_qualname, bare_call, graph_version) \
             VALUES (1, 1, 1, 'CALLS', 'nowhere', 1, 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO unresolved_references \
                (edge_id, source_symbol_id, file_id, edge_kind, reference_name, name_tail, \
                 reason, graph_version) \
             VALUES (1, 1, 1, 'CALLS', 'nowhere', 'nowhere', 'no_candidates', 1)",
            [],
        )
        .unwrap();

        // A CONFIG_BIND edge (a Bridge Edge kind), also NULL-target with a
        // v18 row -- must keep its placeholder edge (edge_id stays
        // populated) since Bridge Edge kinds are written regardless of
        // resolution.
        conn.execute(
            "INSERT INTO edges \
                (id, file_id, source_symbol_id, kind, target_qualname, graph_version) \
             VALUES (2, 1, 1, 'CONFIG_BIND', 'DatabaseOptions', 1)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO unresolved_references \
                (edge_id, source_symbol_id, file_id, edge_kind, reference_name, name_tail, \
                 reason, graph_version) \
             VALUES (2, 1, 1, 'CONFIG_BIND', 'DatabaseOptions', 'DatabaseOptions', \
                     'no_candidates', 1)",
            [],
        )
        .unwrap();

        // A gap edge: NULL-target, no v18 store row at all -- as if
        // orphaned by an FK ON DELETE SET NULL after this database's last
        // repair pass ran.
        conn.execute(
            "INSERT INTO edges \
                (id, file_id, source_symbol_id, kind, target_qualname, graph_version) \
             VALUES (3, 1, 1, 'CALLS', 'also_nowhere', 1)",
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

        // The plain CALLS edge is gone; its reference lives only in the
        // store now.
        let calls_edge_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM edges WHERE id = 1", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            calls_edge_count, 0,
            "non-Bridge-Edge-kind NULL-target edge must be deleted"
        );

        let (calls_edge_id, calls_reason): (Option<i64>, String) = conn
            .query_row(
                "SELECT edge_id, reason FROM unresolved_references \
                 WHERE edge_kind = 'CALLS' AND reference_name = 'nowhere'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(
            calls_edge_id, None,
            "promoted non-Bridge-Edge-kind row must have no edge_id"
        );
        assert_eq!(calls_reason, "no_candidates");

        // The CONFIG_BIND edge survives, still NULL-target, still linked.
        let config_bind_target: Option<i64> = conn
            .query_row(
                "SELECT target_symbol_id FROM edges WHERE id = 2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(config_bind_target, None);
        let config_bind_edge_id: Option<i64> = conn
            .query_row(
                "SELECT edge_id FROM unresolved_references WHERE edge_kind = 'CONFIG_BIND'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            config_bind_edge_id,
            Some(2),
            "Bridge Edge kind row must keep its placeholder edge"
        );

        // The gap edge (no v18 row) is promoted too, with a best-effort
        // reason -- the next repair pass re-derives the real one.
        let gap_edge_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM edges WHERE id = 3", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(gap_edge_count, 0);
        let gap_store_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM unresolved_references WHERE reference_name = 'also_nowhere'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            gap_store_count, 1,
            "gap edge with no v18 row must still be promoted"
        );

        let violations: i64 = conn
            .query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(violations, 0);
    }

    /// Query-plan `detail` strings (column 3 of `EXPLAIN QUERY PLAN`).
    fn query_plan(conn: &Connection, sql: &str) -> Vec<String> {
        let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
        stmt.query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }

    fn index_exists(conn: &Connection, name: &str) -> bool {
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?",
            [name],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    }

    #[test]
    fn migration_25_indexes_unresolved_references_fk_columns_on_an_old_db() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        open_v18_db(&conn);
        assert!(!index_exists(&conn, "idx_unresolved_references_source"));
        assert!(!index_exists(&conn, "idx_unresolved_references_file"));

        migrate(&conn).unwrap();

        assert!(index_exists(&conn, "idx_unresolved_references_source"));
        assert!(index_exists(&conn, "idx_unresolved_references_file"));
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
    fn migration_27_collapses_duplicate_unresolved_rows_and_enforces_uniqueness() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();
        // Rewind to a pre-27 database that already holds duplicates.
        conn.execute(
            "INSERT INTO files (id, path, hash, language, size, modified)
             VALUES (1, 'a.py', 'h', 'python', 1, 1)",
            [],
        )
        .unwrap();
        conn.execute_batch(
            "DROP INDEX idx_unresolved_references_identity;
             UPDATE meta SET value = '26' WHERE key = 'schema_version';",
        )
        .unwrap();
        let insert = |gv: i64, name: &str, line: i64| {
            conn.execute(
                "INSERT INTO unresolved_references
                    (source_symbol_id, file_id, edge_kind, reference_name, name_tail,
                     reason, evidence_start_line, evidence_end_line, graph_version)
                 VALUES (NULL, 1, 'ROUTE', ?, ?, 'no_candidates', ?, ?, ?)",
                rusqlite::params![name, name, line, line, gv],
            )
            .unwrap();
        };
        // gv 1: one reference stored three times; a distinct one once.
        for _ in 0..3 {
            insert(1, "/a/b", 3);
        }
        insert(1, "/a/c", 4);
        // gv 2 keeps its own copy (versions are separate by design).
        insert(2, "/a/b", 3);
        insert(2, "/a/b", 3);

        migrate(&conn).unwrap();

        let count = |gv: i64| -> i64 {
            conn.query_row(
                "SELECT COUNT(*) FROM unresolved_references WHERE graph_version = ?",
                [gv],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(count(1), 2);
        assert_eq!(count(2), 1);
        assert!(index_exists(&conn, "idx_unresolved_references_identity"));

        // A further duplicate is rejected by the constraint, merged by the
        // store's own `ON CONFLICT DO NOTHING` insert.
        let dup = "INSERT INTO unresolved_references
                (source_symbol_id, file_id, edge_kind, reference_name, name_tail,
                 reason, evidence_start_line, evidence_end_line, graph_version)
             VALUES (NULL, 1, 'ROUTE', '/a/b', '/a/b', 'no_candidates', 3, 3, 1)";
        assert!(conn.execute(dup, []).is_err());
        conn.execute(&format!("{dup} ON CONFLICT DO NOTHING"), [])
            .unwrap();
        assert_eq!(count(1), 2);
    }

    #[test]
    fn unresolved_references_fk_lookups_use_an_index_not_a_scan() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();

        for (column, index) in [
            ("source_symbol_id", "idx_unresolved_references_source"),
            ("file_id", "idx_unresolved_references_file"),
        ] {
            let plan = query_plan(
                &conn,
                &format!("SELECT id FROM unresolved_references WHERE {column} = 123"),
            );
            assert!(
                plan.iter()
                    .any(|d| d.contains("SEARCH") && d.contains(index)),
                "{column} lookup must SEARCH {index}, got {plan:?}"
            );
        }
    }

    /// Every FK's referencing column must lead some index, or each parent
    /// delete (a graph-version prune) full-scans the child table. SQLite's
    /// own FK enforcement does exactly that lookup.
    #[test]
    fn every_fk_column_on_edges_and_unresolved_references_is_indexed() {
        let conn = Connection::open_in_memory().unwrap();
        migrate(&conn).unwrap();

        for table in ["edges", "unresolved_references"] {
            let mut fks = conn
                .prepare(&format!(
                    "SELECT \"from\" FROM pragma_foreign_key_list('{table}')"
                ))
                .unwrap();
            let columns: Vec<String> = fks
                .query_map([], |row| row.get(0))
                .unwrap()
                .collect::<rusqlite::Result<_>>()
                .unwrap();
            assert!(!columns.is_empty(), "{table} should declare foreign keys");
            for column in columns {
                let covered: i64 = conn
                    .query_row(
                        &format!(
                            "SELECT COUNT(*) FROM pragma_index_list('{table}') il
                             JOIN pragma_index_info(il.name) ii
                             WHERE ii.seqno = 0 AND ii.name = ?"
                        ),
                        [&column],
                        |row| row.get(0),
                    )
                    .unwrap();
                assert!(
                    covered > 0,
                    "{table}.{column} has a foreign key but no index leading with it"
                );
            }
        }
    }
}
