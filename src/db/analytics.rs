use super::{
    Db, append_path_filters, edge_from_row, extract_target_name, symbol_from_row,
    symbol_from_row_offset,
};
use crate::model::{DuplicateGroup, Edge, Symbol, SymbolComplexity, SymbolCoupling};
use anyhow::Result;

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
        let mut by_module: std::collections::HashMap<String, Vec<(Symbol, i64)>> =
            std::collections::HashMap::new();
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
                          e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                          e.resolution_confidence
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
        let mut params: Vec<&dyn rusqlite::ToSql> =
            vec![&graph_version, &graph_version, &graph_version];

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

        full_sql.push_str(" ORDER BY f.path, e.evidence_start_line LIMIT ?");
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
