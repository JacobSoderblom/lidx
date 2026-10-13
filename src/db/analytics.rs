use super::overview::module_prefix;
use super::public_api::{Candidate, Exposure, PublicApi};
use super::resolver::{ImplicitRootFacts, profile_for, qualname_trailing_name};
use super::{Db, append_path_filters, edge_from_row, extract_target_name, symbol_from_row};
use crate::edge_views;
use crate::model::{
    DeadConfidence, DeadSymbol, DeadSymbols, DuplicateGroup, Edge, Symbol, SymbolComplexity,
    SymbolCoupling, UnresolvedSample,
};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;

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
    /// Pending `unresolved_references` per source symbol (issue #410): the
    /// outgoing references of each of `symbol_ids` (limited to `edge_kinds`)
    /// the write path could not attribute a target to. This is what lets a
    /// `trace_flow` leaf be told apart from a function that genuinely calls
    /// nothing. Symbols with no pending row are absent from the map.
    pub fn unresolved_references_by_source(
        &self,
        symbol_ids: &[i64],
        edge_kinds: &[String],
        graph_version: i64,
    ) -> Result<HashMap<i64, UnresolvedSample>> {
        let mut out: HashMap<i64, UnresolvedSample> = HashMap::new();
        if symbol_ids.is_empty() || edge_kinds.is_empty() {
            return Ok(out);
        }
        let conn = self.read_conn()?;
        let kind_marks = vec!["?"; edge_kinds.len()].join(",");
        for chunk in symbol_ids.chunks(400) {
            let id_marks = vec!["?"; chunk.len()].join(",");
            let sql = format!(
                "SELECT source_symbol_id, COALESCE(reference_name, name_tail), reason \
                 FROM unresolved_references \
                 WHERE source_symbol_id IN ({id_marks}) AND graph_version = ? \
                   AND edge_kind IN ({kind_marks}) \
                 ORDER BY source_symbol_id, id"
            );
            let mut params: Vec<&dyn rusqlite::ToSql> =
                chunk.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
            params.push(&graph_version);
            params.extend(edge_kinds.iter().map(|k| k as &dyn rusqlite::ToSql));
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(&*params, |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            })?;
            for row in rows {
                let (id, name, reason) = row?;
                out.entry(id).or_default().push(name, reason);
            }
        }
        Ok(out)
    }

    /// Upstream analogue of [`Self::unresolved_references_by_source`] (issue
    /// #410): other symbols with an `ambiguous` unresolved reference whose
    /// name tail is `symbol_name` in the same file language -- callers that
    /// may exist but never bound because the name has several candidates.
    /// Counted per DISTINCT source symbol (named by qualname), not per row,
    /// and returned with the number of function/method definitions of that
    /// name in the language so the caller can discard a match that is mostly
    /// noise. Matched by name only, so these are *possible* callers.
    /// `symbol_id` itself is excluded (recursion is not a caller gap).
    pub fn unresolved_callers_of(
        &self,
        symbol_id: i64,
        symbol_name: &str,
        file_path: &str,
        edge_kinds: &[String],
        graph_version: i64,
    ) -> Result<(UnresolvedSample, usize)> {
        let mut sample = UnresolvedSample::default();
        if edge_kinds.is_empty() || symbol_name.is_empty() {
            return Ok((sample, 0));
        }
        let kind_marks = vec!["?"; edge_kinds.len()].join(",");
        let sql = format!(
            "SELECT s.qualname, MIN(ur.reason) \
             FROM unresolved_references ur \
             JOIN files f ON f.id = ur.file_id \
             JOIN symbols s ON s.id = ur.source_symbol_id \
             WHERE ur.name_tail = ? AND ur.graph_version = ? AND ur.reason = 'ambiguous' \
               AND ur.source_symbol_id != ? \
               AND ur.edge_kind IN ({kind_marks}) \
               AND f.language = (SELECT language FROM files WHERE path = ?) \
             GROUP BY ur.source_symbol_id \
             ORDER BY MIN(ur.id)"
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&symbol_name, &graph_version, &symbol_id];
        params.extend(edge_kinds.iter().map(|k| k as &dyn rusqlite::ToSql));
        params.push(&file_path);
        let conn = self.read_conn()?;
        {
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(&*params, |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?;
            for row in rows {
                let (name, reason) = row?;
                sample.push(name, reason);
            }
        }
        if sample.count == 0 {
            return Ok((sample, 0));
        }
        let definitions: i64 = conn.query_row(
            "SELECT COUNT(*) FROM symbols s JOIN files f ON f.id = s.file_id \
             WHERE s.name = ? AND s.graph_version = ? AND s.kind IN ('function', 'method') \
               AND f.language = (SELECT language FROM files WHERE path = ?)",
            rusqlite::params![symbol_name, graph_version, file_path],
            |r| r.get(0),
        )?;
        Ok((sample, definitions as usize))
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
             WHERE e.kind IN ('CALLS', 'DISPATCH')
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
             WHERE e.kind IN ('CALLS', 'DISPATCH')
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
             WHERE e.kind IN ('CALLS', 'DISPATCH')
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
        let is_local =
            super::graph_query::has_modifier_sql("s", super::graph_query::LOCAL_MODIFIER);
        let mut sql = format!(
            "SELECT
                s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                s.graph_version, s.commit_sha, s.stable_id,
                COUNT(e.id) as fan_in
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             LEFT JOIN edges e ON e.target_symbol_id = s.id AND e.kind IN ('CALLS', 'DISPATCH') AND e.graph_version = ?
             WHERE s.graph_version = ?
               AND s.kind IN ('function','method','class','struct','interface','service')
               AND NOT {is_local}
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)"
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
        let is_local =
            super::graph_query::has_modifier_sql("s", super::graph_query::LOCAL_MODIFIER);
        let mut sql = format!(
            "SELECT s.kind, COUNT(*) as cnt
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
               AND s.kind != 'external'
               -- A function declared inside another is no top-level symbol.
               AND NOT {is_local}"
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
        repo_root: &Path,
        include_public_api: bool,
    ) -> Result<DeadSymbols> {
        // Issue #122: impl methods called only through their interface are live.
        let dispatch_from = super::graph_query::dispatch_pairs_from(
            graph_version,
            &super::graph_query::DispatchSeed::All,
        );
        let gv = graph_version;
        let call_reaches = super::graph_query::call_reaches_impl_sql("ce");
        let is_override = super::graph_query::has_modifier_sql("s", "override");
        let is_private = super::graph_query::has_modifier_sql("s", "private");
        // A function declared inside another is only reachable through its
        // parent, which is judged itself.
        let is_local =
            super::graph_query::has_modifier_sql("s", super::graph_query::LOCAL_MODIFIER);
        // A bodiless member declaration has no code to be dead.
        let is_decl = super::graph_query::has_modifier_sql("s", super::graph_query::DECL_MODIFIER);
        let external_base = super::graph_query::external_base_member_sql("s", gv);
        let live_member = super::graph_query::live_nested_member_sql("s", gv);
        let reachability = edge_views::sql_list(edge_views::REACHABILITY);
        let entrypoint = edge_views::sql_list(edge_views::ENTRYPOINT);
        let sql = format!("SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                          s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                          s.graph_version, s.commit_sha, s.stable_id,
                          f.language, s.visibility,
                          (SELECT pc.source_symbol_id FROM edges pc
                           WHERE pc.target_symbol_id = s.id AND pc.kind = 'CONTAINS'
                             AND pc.graph_version = {gv} LIMIT 1),
                          s.kind = 'method' AND f.language = 'rust' AND EXISTS (
                            SELECT 1 FROM edges xe JOIN symbols xs ON xs.id = xe.target_symbol_id
                            WHERE xe.source_symbol_id = s.id AND xe.kind = 'IMPLEMENTS'
                              AND xe.graph_version = {gv} AND xs.kind = 'external')
                   FROM symbols s
                   JOIN files f ON s.file_id = f.id
                   WHERE s.graph_version = ?
                     AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                     AND s.kind IN ('function', 'method', 'class', 'struct', 'const', 'type')
                     -- Module-level JS/TS constants and type aliases only: a
                     -- Rust `const`/`type` or a nested one is judged elsewhere.
                     AND NOT (s.kind IN ('const', 'type') AND NOT (
                       f.language IN ('javascript', 'typescript', 'tsx')
                       AND EXISTS (
                         SELECT 1 FROM edges c
                         JOIN symbols p ON p.id = c.source_symbol_id
                         WHERE c.target_symbol_id = s.id AND c.kind = 'CONTAINS'
                           AND p.kind = 'module' AND c.graph_version = {gv}
                       )
                     ))
                     -- Next.js route-segment exports are read by the framework.
                     AND NOT (f.language IN ('javascript', 'typescript', 'tsx')
                              AND s.name IN ('metadata', 'generateMetadata', 'viewport',
                                             'generateViewport', 'revalidate', 'dynamic',
                                             'dynamicParams', 'fetchCache', 'runtime', 'config',
                                             'preferredRegion', 'maxDuration',
                                             'generateStaticParams')
                              AND (f.path LIKE 'app/%' OR f.path LIKE '%/app/%'
                                   OR f.path LIKE 'pages/%' OR f.path LIKE '%/pages/%'))
                     AND NOT {is_local}
                     AND NOT {is_decl}
                     AND s.name NOT IN ('main', '__init__', 'setup', 'teardown', 'configure', 'register', '.cctor')
                     -- Operators and finalizers are invoked implicitly (#247).
                     AND NOT (f.language = 'csharp' AND (s.name LIKE 'operator %'
                              OR s.name LIKE 'implicit operator %'
                              OR s.name LIKE 'explicit operator %'
                              OR s.name LIKE '~%'))
                     AND COALESCE(s.signature, '') NOT LIKE '%#[test]%'
                     AND COALESCE(s.signature, '') NOT LIKE '%::test]%'
                     AND COALESCE(s.signature, '') NOT LIKE '%#[rstest]%'
                     AND COALESCE(s.signature, '') NOT LIKE '%#[test_case]%'
                     AND NOT EXISTS (
                       SELECT 1 FROM edges e
                       WHERE e.target_symbol_id = s.id
                         AND e.kind IN ({reachability})
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
                         AND e.kind IN ({entrypoint})
                         AND e.graph_version = ?
                     )
                     -- A call the resolver could not narrow (several same-named
                     -- candidates, untyped receiver) calls one of them: none of
                     -- the candidates is provably dead.
                     AND NOT (s.kind IN ('method', 'function') AND EXISTS (
                       SELECT 1 FROM unresolved_references ur
                       JOIN files uf ON uf.id = ur.file_id
                       WHERE ur.name_tail = s.name
                         AND ur.reason = 'ambiguous'
                         AND ur.edge_kind IN ('CALLS', 'REFERENCES')
                         AND ur.graph_version = {gv}
                         AND uf.language = f.language
                     ))
                     AND NOT EXISTS (
                       SELECT 1 {dispatch_from}
                         AND cm.id = s.id
                         AND EXISTS (
                           SELECT 1 FROM edges ce
                           WHERE ce.target_symbol_id = im.id AND ce.kind IN ('CALLS', 'DISPATCH')
                             AND ce.graph_version = {gv}
                             AND {call_reaches}
                         )
                     )
                     AND NOT (s.kind IN ('method', 'function') AND (
                       -- A Rust impl method of an external trait is judged
                       -- afterwards: live only with its type (#472).
                       EXISTS (
                         SELECT 1 FROM edges e
                         LEFT JOIN symbols et ON et.id = e.target_symbol_id
                         WHERE e.source_symbol_id = s.id
                           AND e.kind = 'IMPLEMENTS'
                           AND e.graph_version = ?
                           AND NOT (f.language = 'rust' AND et.kind = 'external')
                       )
                       OR EXISTS (
                         SELECT 1 FROM unresolved_references ur
                         WHERE ur.source_symbol_id = s.id
                           AND ur.edge_kind = 'IMPLEMENTS'
                           AND ur.graph_version = ?
                       )
                     ))
                     -- Issue #238: overrides, framework-called members of a C#
                     -- type with an external base, and types with a live
                     -- member are not dead (see the helpers' docs).
                     AND NOT (s.kind IN ('method', 'function') AND {is_override})
                     AND NOT (s.kind = 'method' AND f.language = 'csharp'
                              AND NOT {is_private} AND {external_base})
                     AND NOT (s.kind IN ('class', 'struct', 'record', 'interface')
                              AND {live_member})");

        let mut full_sql = sql;
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![
            &graph_version,
            &graph_version,
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

        // No SQL `LIMIT`: public-API candidates are set aside afterwards, so
        // the limit applies to what is reported.
        full_sql.push_str(" ORDER BY s.qualname");

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&full_sql)?;
        let rows = stmt.query_map(&*params, |row| {
            Ok((
                symbol_from_row(row)?,
                row.get::<_, Option<String>>(11)?.unwrap_or_default(),
                row.get::<_, String>(16)?,
                row.get::<_, Option<String>>(17)?,
                row.get::<_, Option<i64>>(18)?,
                row.get::<_, bool>(19)?,
            ))
        })?;
        let mut candidates = Vec::new();
        for row in rows {
            candidates.push(row?);
        }
        drop(stmt);

        let mut js_files = HashSet::new();
        let mut rust_libs = HashSet::new();
        let paths_of = |sql: &str| -> Result<HashSet<String>> {
            let mut stmt = conn.prepare(sql)?;
            let rows = stmt.query_map([graph_version], |r| r.get::<_, String>(0))?;
            let mut out = HashSet::new();
            for row in rows {
                out.insert(row?);
            }
            Ok(out)
        };
        let has_language =
            |names: &[&str]| candidates.iter().any(|c| names.contains(&c.2.as_str()));
        if has_language(&["javascript", "typescript", "tsx"]) {
            js_files = paths_of(
                "SELECT path FROM files WHERE language IN ('javascript', 'typescript', 'tsx')
                   AND (deleted_version IS NULL OR deleted_version > ?1)",
            )?;
        }
        if has_language(&["rust"]) {
            rust_libs = paths_of(
                "SELECT path FROM files WHERE language = 'rust'
                   AND (path = 'src/lib.rs' OR path LIKE '%/src/lib.rs')
                   AND (deleted_version IS NULL OR deleted_version > ?1)",
            )?;
        }
        // Names some unresolved reference points at, with the lowest and
        // highest id of the symbols making them (-1: none), computed once.
        let unresolved_names: HashMap<String, (i64, i64)> = {
            let sql = format!(
                "SELECT name_tail, MIN(COALESCE(source_symbol_id, -1)),
                        MAX(COALESCE(source_symbol_id, -1))
                 FROM unresolved_references
                 WHERE graph_version = ?1 AND edge_kind IN ({reachability})
                 GROUP BY name_tail"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([graph_version], |r| {
                Ok((r.get::<_, String>(0)?, (r.get(1)?, r.get(2)?)))
            })?;
            rows.collect::<rusqlite::Result<_>>()?
        };
        let mut api = PublicApi::new(repo_root, js_files, rust_libs);

        // Judged in two passes: a method is live through an external
        // interface only when its type is not itself a claim (#472).
        let mut judged: Vec<Judged> = Vec::new();
        for (symbol, signature, language, visibility, parent_id, external_trait_impl) in candidates
        {
            let profile = profile_for(&language);
            if let Some(is_root) = profile.implicit_root
                && is_root(&ImplicitRootFacts {
                    kind: &symbol.kind,
                    name: &symbol.name,
                    path: &symbol.file_path,
                    signature: &signature,
                    visibility: visibility.as_deref(),
                })
            {
                continue;
            }
            // A reference the symbol makes to itself does not count.
            let unresolved = unresolved_names
                .get(&symbol.name)
                .is_some_and(|&(lo, hi)| !(lo == symbol.id && hi == symbol.id));
            let parent_visibility = if symbol.kind == "method" {
                parent_visibility_of(&conn, &symbol, graph_version)?
            } else {
                None
            };
            let exposure = api.classify(&Candidate {
                language: &language,
                path: &symbol.file_path,
                kind: &symbol.kind,
                name: &symbol.name,
                qualname: &symbol.qualname,
                visibility: visibility.as_deref(),
                parent_visibility: parent_visibility.as_deref(),
            });
            let named = if unresolved {
                "; an unresolved reference names it"
            } else {
                ""
            };
            let (confidence, reason, public) = match exposure {
                Exposure::PublicApi(why) => {
                    (DeadConfidence::Low, format!("public API: {why}"), true)
                }
                Exposure::Internal(why) if unresolved => (
                    DeadConfidence::Medium,
                    format!("{why}, no incoming reference{named}"),
                    false,
                ),
                Exposure::Internal(why) => (
                    DeadConfidence::High,
                    format!("{why}, no incoming reference, no unresolved reference names it"),
                    false,
                ),
                Exposure::ExportedUnimportable(why) => (
                    DeadConfidence::Medium,
                    format!("{why}, no incoming reference{named}"),
                    false,
                ),
                Exposure::Unmodelled => (
                    DeadConfidence::Medium,
                    format!("visibility not modelled for {language}, no incoming reference{named}"),
                    false,
                ),
            };
            judged.push(Judged {
                claim: DeadSymbol {
                    symbol,
                    confidence,
                    reason,
                },
                signature,
                language,
                public,
                named_by_unresolved: unresolved,
                parent_id,
                external_trait_impl,
            });
        }

        // A type an unresolved reference names is not proven dead (a
        // function-local `struct V;` passed as a value, say): its methods
        // keep the benefit of the doubt.
        let claimed: HashSet<i64> = judged
            .iter()
            .filter(|j| !j.public && !j.named_by_unresolved)
            .map(|j| j.claim.symbol.id)
            .collect();
        let mut probe = ExternalInterfaceProbe {
            conn: &conn,
            graph_version,
            claimed: &claimed,
            reachability: &reachability,
            repo_root,
            live: None,
            implemented: HashMap::new(),
            siblings: HashMap::new(),
        };
        let mut out = DeadSymbols::default();
        for mut j in judged {
            if j.claim.symbol.kind == "method"
                && let Some(iface) = probe.of(&j)?
            {
                // Live through its type: not a claim. Listed on request only,
                // never above medium.
                out.external_interface_total += 1;
                if out.external_interface.len() < limit {
                    j.claim.confidence = DeadConfidence::Medium;
                    j.claim.reason = format!(
                        "{}: implements {iface} on a live type",
                        super::external_ifaces::REASON
                    );
                    out.external_interface.push(j.claim);
                }
                continue;
            }
            if j.public {
                out.public_api_total += 1;
                if include_public_api && out.public_api.len() < limit {
                    out.public_api.push(j.claim);
                }
            } else if out.dead.len() < limit {
                out.dead.push(j.claim);
            }
        }
        Ok(out)
    }
}

/// Finds the external interface that makes a method live through its type
/// (`external_ifaces`), caching what it reads.
struct ExternalInterfaceProbe<'a> {
    conn: &'a rusqlite::Connection,
    graph_version: i64,
    claimed: &'a HashSet<i64>,
    reachability: &'a str,
    repo_root: &'a Path,
    /// Computed on the first method that needs it.
    live: Option<HashSet<i64>>,
    implemented: HashMap<i64, Vec<String>>,
    siblings: HashMap<i64, Vec<(String, Option<String>)>>,
}

impl ExternalInterfaceProbe<'_> {
    /// The interface method `j` implements, or `None` when it implements none
    /// or its type is not live. A method whose type cannot be found is given
    /// the benefit of the doubt.
    fn of(&mut self, j: &Judged) -> Result<Option<String>> {
        let (conn, graph_version) = (self.conn, self.graph_version);
        if let Some(parent) = j.parent_id {
            if self.live.is_none() {
                self.live = Some(live_symbols(
                    conn,
                    graph_version,
                    self.reachability,
                    self.claimed,
                    self.repo_root,
                )?);
            }
            if !self.live.as_ref().is_some_and(|l| l.contains(&parent)) {
                return Ok(None);
            }
        }
        let method = &j.claim.symbol;
        let found = match j.language.as_str() {
            "rust" => j.external_trait_impl.then(|| "external trait".to_string()),
            "go" => {
                let Some(parent) = j.parent_id else {
                    return Ok(None);
                };
                if let std::collections::hash_map::Entry::Vacant(slot) = self.siblings.entry(parent)
                {
                    let mut stmt = conn.prepare_cached(
                        "SELECT m.name, m.signature FROM edges c
                         JOIN symbols m ON m.id = c.target_symbol_id
                         WHERE c.source_symbol_id = ?1 AND c.kind = 'CONTAINS'
                           AND c.graph_version = ?2 AND m.kind = 'method'",
                    )?;
                    let rows = stmt
                        .query_map(rusqlite::params![parent, graph_version], |r| {
                            Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    slot.insert(rows);
                }
                super::resolver::go_external_interface(
                    &method.name,
                    Some(j.signature.as_str()),
                    &self.siblings[&parent],
                )
            }
            "typescript" | "tsx" | "javascript" => {
                let names: &[String] = match j.parent_id {
                    Some(parent) => {
                        if let std::collections::hash_map::Entry::Vacant(slot) =
                            self.implemented.entry(parent)
                        {
                            let mut stmt = conn.prepare_cached(
                                "SELECT reference_name FROM unresolved_references
                                 WHERE source_symbol_id = ?1 AND edge_kind = 'IMPLEMENTS'
                                   AND graph_version = ?2 AND reference_name IS NOT NULL",
                            )?;
                            let rows = stmt
                                .query_map(rusqlite::params![parent, graph_version], |r| {
                                    r.get::<_, String>(0)
                                })?
                                .collect::<rusqlite::Result<Vec<_>>>()?;
                            slot.insert(rows);
                        }
                        &self.implemented[&parent]
                    }
                    None => &[],
                };
                super::external_ifaces::ts_external_interface(&method.name, names)
            }
            _ => None,
        };
        Ok(found)
    }
}

impl Db {
    /// Issue #79: an IMPORTS reference the write path never resolved (an
    /// external package, e.g. `import json`) no longer gets a placeholder
    /// edge at all -- it lives only in `unresolved_references` (`edge_id IS
    /// NULL`). Unioned in here as a second branch so it's still reported as
    /// unused when nothing calls it, with its `id` negated (`-ur.id`) to
    /// keep it visibly distinct from a real `edges.id` -- a pending row has
    /// no edge of its own to report the id of.
    ///
    /// Issue #116: "is it used" is no longer a same-file `CALLS` edge with
    /// an *identical* `target_qualname` string -- a Python call's
    /// `target_qualname` is the extractor's own local guess (e.g. a bare
    /// `helper_used()` call becomes `<this module>.helper_used` regardless
    /// of which module actually defines it), so it essentially never equals
    /// the IMPORTS edge's own `target_qualname`, and an attribute use
    /// (`json.dumps(...)`) or an annotation-only use never had a matching
    /// `CALLS` edge at all. `import_alias_used` below now checks, in order:
    /// (1) a resolved import's `target_symbol_id` matching any other
    /// same-file edge's resolved target -- the precise case, e.g. `from
    /// pkg.utils import helper_used` + a same-file call that resolves to
    /// that same symbol despite its own guessed text differing; (2) the
    /// bound name (the IMPORTS edge's `bound_name` detail, so `import numpy as np`
    /// looks for `np`; else the target's trailing segment) occurring as a
    /// same-file reference's own name, its own bare form, or the leading or
    /// trailing dotted segment of one -- covers both the bare-call-guess
    /// mismatch above and attribute access on an unresolved external import
    /// (`import json` + `json.dumps(...)`); (3) that same bound name
    /// occurring as a token in a same-file symbol's `signature` -- the
    /// annotation-only case (`def f(x: Optional[int])`), which never emits
    /// any edge at all; (4) a module-level `__all__` re-export of that name
    /// (`python::emit_module_export_edges`, `MODULE_EXPORT_KIND`); and, for
    /// Python files only (issue #242), (5) a whole-identifier scan of the
    /// file's code (`indexer::python::file_identifiers`) -- value uses such as decorators, arguments
    /// and attribute reads emit no edge -- excluding import statements only
    /// (comments and strings count as mentions).
    pub fn unused_imports(
        &self,
        limit: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
        repo_root: &Path,
    ) -> Result<Vec<Edge>> {
        let mut full_sql = String::from(
            "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                    e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.resolution_kind, e.file_id, f.language
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE e.kind = 'IMPORTS'
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
               AND e.target_qualname IS NOT NULL",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];

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
                    NULL, ur.file_id, f.language
             FROM unresolved_references ur
             JOIN files f ON ur.file_id = f.id
             WHERE ur.edge_kind = 'IMPORTS'
               AND ur.edge_id IS NULL
               AND ur.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
               AND ur.reference_name IS NOT NULL",
        );
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
        // No LIMIT here: `limit` counts *unused* imports, decided below only
        // after each candidate is checked against same-file usage signals.
        full_sql.push_str(" ORDER BY 2, 9");

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&full_sql)?;
        let candidates: Vec<(Edge, i64, String)> = stmt
            .query_map(&*params, |row| {
                let edge = edge_from_row(row)?;
                let file_id: i64 = row.get(17)?;
                let language: String = row.get(18)?;
                Ok((edge, file_id, language))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        if candidates.is_empty() {
            return Ok(Vec::new());
        }

        let file_ids: Vec<i64> = candidates
            .iter()
            .map(|(_, file_id, _)| *file_id)
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let usage = self.file_usage_signals(&file_ids, graph_version)?;

        // Issue #242: per-file Python name scans, read lazily and cached.
        let mut py_names: HashMap<String, Option<HashSet<String>>> = HashMap::new();
        let mut results = Vec::new();
        for (edge, file_id, language) in candidates {
            if results.len() >= limit {
                break;
            }
            let Some(target_qualname) = edge.target_qualname.as_deref() else {
                continue;
            };
            let bound_name = edge
                .detail
                .as_deref()
                .and_then(|detail| serde_json::from_str::<serde_json::Value>(detail).ok())
                .and_then(|detail| detail["bound_name"].as_str().map(String::from));
            let alias = bound_name
                .as_deref()
                .unwrap_or_else(|| qualname_trailing_name(target_qualname));
            if import_alias_used(alias, edge.target_symbol_id, file_id, &usage) {
                continue;
            }
            // A Go call through an import is recorded under the import path
            // (`path/filepath.Join`), whatever alias the file binds.
            if language == "go"
                && usage.get(&file_id).is_some_and(|u| {
                    let prefix = format!("{target_qualname}.");
                    u.names.iter().any(|name| name.starts_with(&prefix))
                })
            {
                continue;
            }
            // Issue #242: an unreadable file counts as used (safe direction).
            if language == "python"
                && py_names
                    .entry(edge.file_path.clone())
                    .or_insert_with(|| {
                        crate::indexer::python::file_identifiers(repo_root, &edge.file_path)
                    })
                    .as_ref()
                    .is_none_or(|names| names.contains(alias))
            {
                continue;
            }
            results.push(edge);
        }
        Ok(results)
    }

    /// Same-file "this name/symbol is referenced somewhere" signals for
    /// `unused_imports`, gathered once for every file with at least one
    /// IMPORTS candidate rather than per-candidate (issue #116). `names`
    /// covers both resolved edges (any kind but the import machinery
    /// itself) and still-unresolved references (including a module's own
    /// `MODULE_EXPORT_KIND` `__all__` entries); `signatures` covers
    /// annotation-only uses, which never emit an edge at all.
    fn file_usage_signals(
        &self,
        file_ids: &[i64],
        graph_version: i64,
    ) -> Result<HashMap<i64, FileUsage>> {
        let mut usage: HashMap<i64, FileUsage> = HashMap::new();
        if file_ids.is_empty() {
            return Ok(usage);
        }
        let conn = self.read_conn()?;
        let placeholders = file_ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");

        let edges_sql = format!(
            "SELECT file_id, target_qualname, target_symbol_id FROM edges
             WHERE graph_version = ?
               AND kind NOT IN ({non_usage})
               AND file_id IN ({placeholders})",
            non_usage = edge_views::sql_list(edge_views::NON_USAGE)
        );
        let mut edges_params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version];
        for id in file_ids {
            edges_params.push(id as &dyn rusqlite::ToSql);
        }
        let mut edges_stmt = conn.prepare(&edges_sql)?;
        let edge_rows = edges_stmt.query_map(&*edges_params, |row| {
            let file_id: i64 = row.get(0)?;
            let target_qualname: Option<String> = row.get(1)?;
            let target_symbol_id: Option<i64> = row.get(2)?;
            Ok((file_id, target_qualname, target_symbol_id))
        })?;
        for row in edge_rows {
            let (file_id, target_qualname, target_symbol_id) = row?;
            let entry = usage.entry(file_id).or_default();
            if let Some(name) = target_qualname {
                entry.names.push(name);
            }
            if let Some(id) = target_symbol_id {
                entry.symbol_ids.insert(id);
            }
        }

        let ur_sql = format!(
            "SELECT file_id, reference_name FROM unresolved_references
             WHERE graph_version = ?
               AND edge_kind NOT IN ({import_machinery})
               AND reference_name IS NOT NULL
               AND file_id IN ({placeholders})",
            import_machinery = edge_views::sql_list(edge_views::IMPORT_MACHINERY)
        );
        let mut ur_params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version];
        for id in file_ids {
            ur_params.push(id as &dyn rusqlite::ToSql);
        }
        let mut ur_stmt = conn.prepare(&ur_sql)?;
        let ur_rows = ur_stmt.query_map(&*ur_params, |row| {
            let file_id: i64 = row.get(0)?;
            let reference_name: String = row.get(1)?;
            Ok((file_id, reference_name))
        })?;
        for row in ur_rows {
            let (file_id, reference_name) = row?;
            usage.entry(file_id).or_default().names.push(reference_name);
        }

        let sig_sql = format!(
            "SELECT file_id, signature FROM symbols
             WHERE graph_version = ?
               AND signature IS NOT NULL
               AND file_id IN ({placeholders})"
        );
        let mut sig_params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version];
        for id in file_ids {
            sig_params.push(id as &dyn rusqlite::ToSql);
        }
        let mut sig_stmt = conn.prepare(&sig_sql)?;
        let sig_rows = sig_stmt.query_map(&*sig_params, |row| {
            let file_id: i64 = row.get(0)?;
            let signature: String = row.get(1)?;
            Ok((file_id, signature))
        })?;
        for row in sig_rows {
            let (file_id, signature) = row?;
            usage.entry(file_id).or_default().signatures.push(signature);
        }

        Ok(usage)
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

/// Same-file "this name/symbol is referenced somewhere" signals for
/// `Db::unused_imports` (issue #116), gathered once per file rather than
/// per import candidate.
#[derive(Default)]
struct FileUsage {
    /// Reference-name texts to check a bound alias against (equal, or the
    /// alias's leading/trailing dotted segment) -- from
    /// `edges.target_qualname` (any kind but the import machinery itself)
    /// and `unresolved_references.reference_name` (which also carries a
    /// module's own `MODULE_EXPORT_KIND` `__all__` entries).
    names: Vec<String>,
    /// Resolved `target_symbol_id`s any same-file edge points at.
    symbol_ids: HashSet<i64>,
    /// Same-file symbols' `signature` text, tokenized for annotation-only
    /// uses (`def f(x: Optional[int])` never emits an edge at all).
    signatures: Vec<String>,
}

/// Issue #116's "is it used" predicate for one IMPORTS candidate.
/// `alias` is the name the import binds into the file's scope (the IMPORTS
/// edge's `bound_name` detail, else the target's trailing segment);
/// `target_symbol_id` is `Some` only when the import itself resolved to a
/// real symbol.
fn import_alias_used(
    alias: &str,
    target_symbol_id: Option<i64>,
    file_id: i64,
    usage: &HashMap<i64, FileUsage>,
) -> bool {
    let Some(file_usage) = usage.get(&file_id) else {
        return false;
    };
    if let Some(id) = target_symbol_id
        && file_usage.symbol_ids.contains(&id)
    {
        return true;
    }
    if alias.is_empty() {
        return false;
    }
    let prefix = format!("{alias}.");
    let suffix = format!(".{alias}");
    if file_usage
        .names
        .iter()
        .any(|name| name == alias || name.starts_with(&prefix) || name.ends_with(&suffix))
    {
        return true;
    }
    file_usage
        .signatures
        .iter()
        .any(|signature| signature_mentions(signature, alias))
}

/// Whether `alias` occurs as a whole identifier token in `signature` (e.g.
/// `Optional` in `(x: Optional[int]) -> None`) -- not just a substring, so
/// an alias like `Int` doesn't match `MyIntSetting`.
fn signature_mentions(signature: &str, alias: &str) -> bool {
    signature
        .split(|ch: char| !ch.is_alphanumeric() && ch != '_')
        .any(|token| token == alias)
}

/// The `visibility` modifiers of the symbol(s) a method's qualname hangs
/// under (`Type::method` or `pkg.Type.method`), joined by spaces.
/// The symbols reachable from the roots along `reachability` edges. A root
/// is a symbol nothing references that is not a claim (public API, tests,
/// modules, members reached by dispatch or an interface); a symbol referenced
/// only from claims, or from a cluster no root reaches, is not live even
/// though `dead_symbols` finds an incoming edge for it (#472).
///
/// A Go type mention is stored as one `USES` edge per file from the file's
/// module, so the edge alone cannot say whether the mentioning code is live.
/// Those edges are settled last, by reading the file: the type is live when
/// some mention lies in a live declaration (or outside any declaration), and
/// mentions inside the type's own declaration and methods do not count.
fn live_symbols(
    conn: &rusqlite::Connection,
    graph_version: i64,
    reachability: &str,
    claimed: &HashSet<i64>,
    repo_root: &Path,
) -> Result<HashSet<i64>> {
    struct Mention {
        target: i64,
        qualname: String,
        name: String,
        path: String,
    }
    let mut stmt = conn.prepare(&format!(
        "SELECT e.source_symbol_id, e.target_symbol_id, e.kind, ss.kind, f.language, f.path,
                t.name, t.qualname
         FROM edges e
         LEFT JOIN symbols ss ON ss.id = e.source_symbol_id
         JOIN symbols t ON t.id = e.target_symbol_id
         JOIN files f ON f.id = e.file_id
         WHERE e.kind IN ({reachability}) AND e.graph_version = ?1
           AND (e.source_symbol_id IS NULL OR e.source_symbol_id != e.target_symbol_id)"
    ))?;
    let mut out_edges: HashMap<i64, Vec<i64>> = HashMap::new();
    let mut referenced: HashSet<i64> = HashSet::new();
    let mut live: HashSet<i64> = HashSet::new();
    let mut stack: Vec<i64> = Vec::new();
    let mut mentions: Vec<Mention> = Vec::new();
    let rows = stmt.query_map([graph_version], |r| {
        Ok((
            r.get::<_, Option<i64>>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?,
            r.get::<_, Option<String>>(3)?,
            r.get::<_, String>(4)?,
            r.get::<_, String>(5)?,
            r.get::<_, String>(6)?,
            r.get::<_, String>(7)?,
        ))
    })?;
    for row in rows {
        let (source, target, kind, source_kind, language, path, name, qualname) = row?;
        referenced.insert(target);
        match source {
            // not owned by a symbol: a file-level reference
            None => {
                if live.insert(target) {
                    stack.push(target);
                }
            }
            Some(_)
                if kind == "USES"
                    && language == "go"
                    && source_kind.as_deref() == Some("module") =>
            {
                mentions.push(Mention {
                    target,
                    qualname,
                    name,
                    path,
                });
            }
            Some(source) => out_edges.entry(source).or_default().push(target),
        }
    }
    drop(stmt);
    let mut ids = conn.prepare("SELECT id FROM symbols WHERE graph_version = ?1")?;
    let rows = ids.query_map([graph_version], |r| r.get::<_, i64>(0))?;
    for id in rows {
        let id = id?;
        if !referenced.contains(&id) && !claimed.contains(&id) && live.insert(id) {
            stack.push(id);
        }
    }
    drop(ids);

    // file path -> (declarations: id, qualname, first line, last line; text lines)
    type Decls = Vec<(i64, String, i64, i64)>;
    let mut files: HashMap<String, Option<(Decls, Vec<String>)>> = HashMap::new();
    loop {
        while let Some(id) = stack.pop() {
            for &target in out_edges.get(&id).into_iter().flatten() {
                if live.insert(target) {
                    stack.push(target);
                }
            }
        }
        let mut grew = false;
        for m in &mentions {
            if live.contains(&m.target) {
                continue;
            }
            if !files.contains_key(&m.path) {
                let decls: Decls = {
                    let mut stmt = conn.prepare_cached(
                        "SELECT s.id, s.qualname, s.start_line, s.end_line FROM symbols s
                         JOIN files f ON f.id = s.file_id
                         WHERE f.path = ?1 AND s.graph_version = ?2 AND s.kind != 'module'",
                    )?;
                    stmt.query_map(rusqlite::params![m.path, graph_version], |r| {
                        Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
                    })?
                    .collect::<rusqlite::Result<_>>()?
                };
                let text = std::fs::read_to_string(repo_root.join(&m.path)).ok();
                files.insert(
                    m.path.clone(),
                    text.map(|t| (decls, t.lines().map(str::to_string).collect())),
                );
            }
            // No readable source: keep the benefit of the doubt.
            let Some((decls, lines)) = &files[&m.path] else {
                live.insert(m.target);
                stack.push(m.target);
                grew = true;
                continue;
            };
            let own = format!("{}.", m.qualname);
            let mut found = false;
            let mut any = false;
            for (idx, line) in lines.iter().enumerate() {
                if !mentions_word(line, &m.name) {
                    continue;
                }
                any = true;
                let n = idx as i64 + 1;
                let enclosing = decls
                    .iter()
                    .filter(|(_, _, lo, hi)| *lo <= n && n <= *hi)
                    .min_by_key(|(_, _, lo, hi)| hi - lo);
                match enclosing {
                    Some((_, q, _, _)) if *q == m.qualname || q.starts_with(&own) => {}
                    Some((id, _, _, _)) => found |= live.contains(id),
                    None => found = true,
                }
                if found {
                    break;
                }
            }
            if found || !any {
                live.insert(m.target);
                stack.push(m.target);
                grew = true;
            }
        }
        if !grew && stack.is_empty() {
            break;
        }
    }
    Ok(live)
}

/// Whether `line` contains `word` as a whole identifier.
fn mentions_word(line: &str, word: &str) -> bool {
    let ident = |c: char| c.is_alphanumeric() || c == '_';
    line.match_indices(word).any(|(i, _)| {
        !line[..i].chars().next_back().is_some_and(ident)
            && !line[i + word.len()..].chars().next().is_some_and(ident)
    })
}

/// One candidate once classified, before external-interface liveness.
struct Judged {
    claim: DeadSymbol,
    signature: String,
    language: String,
    public: bool,
    /// An unresolved reference names it: it may be used after all.
    named_by_unresolved: bool,
    /// The declaration containing it (a method's type).
    parent_id: Option<i64>,
    /// A Rust method implementing a trait of a dependency or `std`.
    external_trait_impl: bool,
}

fn parent_visibility_of(
    conn: &rusqlite::Connection,
    method: &Symbol,
    graph_version: i64,
) -> Result<Option<String>> {
    let Some(stem) = method.qualname.strip_suffix(method.name.as_str()) else {
        return Ok(None);
    };
    let mut stmt = conn.prepare_cached(
        "SELECT group_concat(COALESCE(visibility, ''), ' ') FROM symbols
         WHERE graph_version = ?1 AND qualname IN (?2, ?3)",
    )?;
    let parent: Option<String> = stmt.query_row(
        rusqlite::params![
            graph_version,
            stem.strip_suffix("::").unwrap_or(""),
            stem.strip_suffix('.').unwrap_or("")
        ],
        |r| r.get(0),
    )?;
    Ok(parent)
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
            identity: None,
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
