use super::overview::module_prefix;
use super::{Db, append_path_filters, edge_from_row, extract_target_name, symbol_from_row};
use crate::model::{DuplicateGroup, Edge, Symbol, SymbolComplexity, SymbolCoupling};
use anyhow::Result;

// Issue #134 follow-up: module identity used to be computed twice in
// `top_fan_in_by_module` -- once here via a raw-SQL "first path segment"
// `CASE` (no trailing separator, and the bare filename for a root-level
// file), and separately in `module_summary` via `module_prefix()`
// (configurable depth, and always a trailing separator, `"./"` for
// root-level files). The two disagreed for root-level files in particular:
// this method used to group `main.rs` under module `"main.rs"` while
// `module_summary` grouped it under `"./"`, so the repo map's
// "## Modules" and "## Key Symbols" sections showed different module
// identities for the same files. Grouping now happens in Rust with the
// same `module_prefix()` `module_summary` uses, at the same depth (1) that
// `repo_map::build_repo_map` passes to `module_summary` -- its only
// caller -- so both sections always agree on what a "module" is.
const KEY_SYMBOLS_MODULE_DEPTH: usize = 1;

impl Db {
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
        let mut params: Vec<&dyn rusqlite::ToSql> =
            vec![&graph_version, &graph_version, &graph_version];

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

        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");
        sql.push_str(" GROUP BY s.id");
        sql.push_str(" HAVING fan_in > 0");
        // Ordered by `fan_in` alone (not per-module) since grouping now
        // happens after the query, in Rust -- see the module-identity note
        // above. A global sort by `fan_in DESC` still leaves every
        // per-module subsequence in `fan_in DESC` order below.
        sql.push_str(" ORDER BY fan_in DESC, s.id");

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, |row| {
            let symbol = symbol_from_row(row)?;
            let fan_in: i64 = row.get(16)?;
            Ok((symbol, fan_in))
        })?;

        // Collect and group by module, limiting per module
        let mut by_module: std::collections::HashMap<String, Vec<(Symbol, i64)>> =
            std::collections::HashMap::new();
        for row in rows {
            let (symbol, fan_in) = row?;
            let module = module_prefix(&symbol.file_path, KEY_SYMBOLS_MODULE_DEPTH);
            by_module.entry(module).or_default().push((symbol, fan_in));
        }

        // Flatten with limit per module. Issue #134: distinct symbols can
        // share a bare name (e.g. a common helper repeated across files in
        // the same top-level module) -- rows are already ordered by
        // `fan_in DESC` per module, so keeping only the first occurrence of
        // each name drops the lower-ranked duplicate while still surfacing
        // the highest-fan-in symbol for that name.
        let mut results = Vec::new();
        for (module, symbols) in by_module {
            let mut seen_names = std::collections::HashSet::new();
            let mut deduped: Vec<(Symbol, i64)> = symbols
                .into_iter()
                .filter(|(symbol, _)| seen_names.insert(symbol.name.clone()))
                .collect();
            deduped.truncate(limit_per_module);
            for (symbol, fan_in) in deduped {
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
        // Issue #80: excludes external stub symbols (`kind = 'external'`)
        // -- they'd otherwise show up as their own noise bucket in the
        // repo map's "Patterns" section.
        let mut sql = String::from(
            "SELECT s.kind, COUNT(*) as cnt
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
               AND s.kind != 'external'",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];

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

    #[allow(clippy::too_many_arguments)]
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
            if let Some(languages) = languages
                && !languages.is_empty()
            {
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
            let rows = member_stmt.query_map(&*member_params, symbol_from_row)?;
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
                     AND COALESCE(s.signature, '') NOT LIKE '%#[test]%'
                     AND COALESCE(s.signature, '') NOT LIKE '%::test]%'
                     AND COALESCE(s.signature, '') NOT LIKE '%#[rstest]%'
                     AND COALESCE(s.signature, '') NOT LIKE '#[trait_method]%'
                     AND NOT EXISTS (
                       SELECT 1 FROM edges e
                       WHERE e.target_symbol_id = s.id
                         AND e.kind IN ('CALLS', 'IMPORTS', 'RPC_IMPL', 'IMPLEMENTS', 'EXTENDS', 'USES')
                         AND e.graph_version = ?
                     )
                     AND NOT EXISTS (
                       SELECT 1 FROM edges e
                       WHERE e.target_symbol_id = s.id
                         AND e.kind = 'IMPORTS'
                         AND e.graph_version = ?
                         AND e.file_id != s.file_id
                     )
                     AND NOT EXISTS (
                       SELECT 1 FROM edges e
                       WHERE e.source_symbol_id = s.id
                         AND e.kind IN ('HTTP_ROUTE', 'RPC_IMPL', 'CHANNEL_SUBSCRIBE')
                         AND e.graph_version = ?
                     )";

        let mut full_sql = String::from(sql);
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![
            &graph_version,
            &graph_version,
            &graph_version,
            &graph_version,
            &graph_version,
        ];

        if let Some(languages) = languages
            && !languages.is_empty()
        {
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

        let mut path_params = Vec::new();
        append_path_filters(&mut full_sql, &mut params, &mut path_params, paths, "f");

        full_sql.push_str(" ORDER BY s.qualname LIMIT ?");
        let limit = limit as i64;
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&full_sql)?;
        let rows = stmt.query_map(&*params, symbol_from_row)?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Issue #79: an IMPORTS reference the write path never resolved (an
    /// external package, e.g. `import json`) no longer gets a placeholder
    /// edge at all -- it lives only in `unresolved_references` (`edge_id IS
    /// NULL`). Unioned in here as a second branch so it's still reported as
    /// unused when nothing calls it, with its `id` negated (`-ur.id`) to
    /// keep it visibly distinct from a real `edges.id` -- a pending row has
    /// no edge of its own to report the id of. The "is it used" check is
    /// unioned the same way on the CALLS side: a call the write path also
    /// left unresolved (`from fastapi import FastAPI` + `FastAPI()`, both
    /// external) is just as real a use as a resolved one, and before issue
    /// #79 both shared one `edges` row's worth of visibility here.
    pub fn unused_imports(
        &self,
        limit: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        let mut full_sql = String::from(
            "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                    e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.resolution_kind
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
               )
               AND NOT EXISTS (
                 SELECT 1 FROM unresolved_references ur2
                 WHERE ur2.edge_kind = 'CALLS'
                   AND ur2.edge_id IS NULL
                   AND ur2.file_id = e.file_id
                   AND ur2.reference_name = e.target_qualname
                   AND ur2.graph_version = ?
               )",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![
            &graph_version,
            &graph_version,
            &graph_version,
            &graph_version,
        ];

        if let Some(languages) = languages
            && !languages.is_empty()
        {
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

        let mut path_params_edges = Vec::new();
        append_path_filters(
            &mut full_sql,
            &mut params,
            &mut path_params_edges,
            paths,
            "f",
        );

        full_sql.push_str(
            " UNION ALL
             SELECT -ur.id, f.path, ur.edge_kind, ur.source_symbol_id, NULL,
                    ur.reference_name, ur.detail, ur.evidence_snippet,
                    ur.evidence_start_line, ur.evidence_end_line, ur.confidence,
                    ur.graph_version, ur.commit_sha, ur.trace_id, ur.span_id, ur.event_ts,
                    NULL
             FROM unresolved_references ur
             JOIN files f ON ur.file_id = f.id
             WHERE ur.edge_kind = 'IMPORTS'
               AND ur.edge_id IS NULL
               AND ur.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
               AND ur.reference_name IS NOT NULL
               AND NOT EXISTS (
                 SELECT 1 FROM edges e2
                 WHERE e2.kind = 'CALLS'
                   AND e2.file_id = ur.file_id
                   AND e2.target_qualname = ur.reference_name
                   AND e2.graph_version = ?
               )
               AND NOT EXISTS (
                 SELECT 1 FROM unresolved_references ur2
                 WHERE ur2.edge_kind = 'CALLS'
                   AND ur2.edge_id IS NULL
                   AND ur2.file_id = ur.file_id
                   AND ur2.reference_name = ur.reference_name
                   AND ur2.graph_version = ?
               )",
        );
        params.push(&graph_version);
        params.push(&graph_version);
        params.push(&graph_version);
        params.push(&graph_version);

        if let Some(languages) = languages
            && !languages.is_empty()
        {
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

        let mut path_params_store = Vec::new();
        append_path_filters(
            &mut full_sql,
            &mut params,
            &mut path_params_store,
            paths,
            "f",
        );

        // Column 2 = file_path, column 9 = evidence_start_line -- an ORDER BY
        // after a UNION ALL can't qualify columns by table alias anymore.
        full_sql.push_str(" ORDER BY 2, 9 LIMIT ?");
        let limit = limit as i64;
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&full_sql)?;
        let rows = stmt.query_map(&*params, edge_from_row)?;
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

        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");

        // Cap scan to limit*10 to avoid unbounded N+1 queries
        let scan_cap = limit * 10;
        sql.push_str(&format!(" ORDER BY s.qualname LIMIT {}", scan_cap));

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, symbol_from_row)?;
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
}

#[cfg(test)]
mod tests {
    use crate::db::Db;
    use crate::indexer::extract::{EdgeInput, SymbolInput};
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn create_test_db() -> (Db, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        let db = Db::new(&db_path).unwrap();
        (db, temp_dir)
    }

    fn make_symbol(qualname: &str, kind: &str) -> SymbolInput {
        SymbolInput {
            kind: kind.to_string(),
            name: qualname
                .split('.')
                .next_back()
                .unwrap_or(qualname)
                .to_string(),
            qualname: qualname.to_string(),
            start_line: 1,
            start_col: 0,
            end_line: 5,
            end_col: 0,
            start_byte: 0,
            end_byte: 50,
            signature: None,
            docstring: None,
        }
    }

    fn make_edge(kind: &str, source: &str, target: &str) -> EdgeInput {
        EdgeInput {
            kind: kind.to_string(),
            source_qualname: Some(source.to_string()),
            target_qualname: Some(target.to_string()),
            ..Default::default()
        }
    }

    // Issue #134: two distinct functions that happen to share a bare name
    // (e.g. a common helper name like `_require` repeated across files in
    // the same top-level module) both land in `top_fan_in_by_module`'s
    // per-module results. Repo map's "Key Symbols" section then lists that
    // name twice under one module header with no way to tell them apart.
    #[test]
    fn top_fan_in_by_module_dedupes_same_name_per_module() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();

        let fid_a = db.upsert_file("pkg/a.py", "h1", "python", 10, 0).unwrap();
        let fid_b = db.upsert_file("pkg/b.py", "h2", "python", 10, 0).unwrap();
        let fid_app = db.upsert_file("app.py", "h3", "python", 10, 0).unwrap();

        let ins_a = db
            .insert_symbols(
                fid_a,
                "pkg/a.py",
                &[make_symbol("pkg.a.helper", "function")],
                gv,
                None,
            )
            .unwrap();
        let ins_b = db
            .insert_symbols(
                fid_b,
                "pkg/b.py",
                &[make_symbol("pkg.b.helper", "function")],
                gv,
                None,
            )
            .unwrap();
        let ins_app = db
            .insert_symbols(
                fid_app,
                "app.py",
                &[make_symbol("app.caller", "function")],
                gv,
                None,
            )
            .unwrap();

        let mut sym_map = HashMap::new();
        sym_map.insert("pkg.a.helper".to_string(), ins_a[0].id);
        sym_map.insert("pkg.b.helper".to_string(), ins_b[0].id);
        sym_map.insert("app.caller".to_string(), ins_app[0].id);
        db.insert_edges(
            fid_app,
            &[
                make_edge("CALLS", "app.caller", "pkg.a.helper"),
                make_edge("CALLS", "app.caller", "pkg.b.helper"),
            ],
            &sym_map,
            gv,
            None,
        )
        .unwrap();

        let results = db.top_fan_in_by_module(10, None, None, gv).unwrap();
        let helper_count = results
            .iter()
            .filter(|(module, sym, _)| module == "pkg/" && sym.name == "helper")
            .count();
        assert_eq!(
            helper_count, 1,
            "expected `helper` to be deduplicated within the `pkg` module, got: {:?}",
            results
        );
    }

    // Issue #134 follow-up: `top_fan_in_by_module` used to group a
    // root-level file (no `/` in its path) under its bare filename (e.g.
    // `"main.rs"`), while `module_summary` -- via `module_prefix()` --
    // grouped it under `"./"`. This left the repo map's "## Modules" and
    // "## Key Symbols" sections disagreeing on root-level module identity.
    // Both now go through `module_prefix()`, so they must agree.
    #[test]
    fn top_fan_in_by_module_groups_root_level_file_as_dot_slash() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();

        let fid_main = db.upsert_file("main.rs", "h1", "rust", 10, 0).unwrap();
        let fid_other = db.upsert_file("other.rs", "h2", "rust", 10, 0).unwrap();

        let ins_main = db
            .insert_symbols(
                fid_main,
                "main.rs",
                &[make_symbol("main.run", "function")],
                gv,
                None,
            )
            .unwrap();
        let ins_other = db
            .insert_symbols(
                fid_other,
                "other.rs",
                &[make_symbol("other.caller", "function")],
                gv,
                None,
            )
            .unwrap();

        let mut sym_map = HashMap::new();
        sym_map.insert("main.run".to_string(), ins_main[0].id);
        sym_map.insert("other.caller".to_string(), ins_other[0].id);
        db.insert_edges(
            fid_other,
            &[make_edge("CALLS", "other.caller", "main.run")],
            &sym_map,
            gv,
            None,
        )
        .unwrap();

        let results = db.top_fan_in_by_module(10, None, None, gv).unwrap();
        let run_entry = results.iter().find(|(_, sym, _)| sym.name == "run");
        assert_eq!(
            run_entry.map(|(module, ..)| module.as_str()),
            Some("./"),
            "expected root-level file to group under the same \"./\" module \
             `module_summary` uses, got: {:?}",
            results
        );
        assert!(
            !results.iter().any(|(module, ..)| module == "main.rs"),
            "root-level file should not be grouped under its bare filename, got: {:?}",
            results
        );
    }
}
