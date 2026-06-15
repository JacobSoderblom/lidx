use super::{Db, edge_from_row, symbol_from_row};
use crate::model::{Edge, Symbol};
use anyhow::Result;
use rusqlite::OptionalExtension;
use std::collections::{HashMap, HashSet};

impl Db {
    pub fn find_symbols(
        &self,
        query: &str,
        limit: usize,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Symbol>> {
        let tokens: Vec<&str> = query.split_whitespace().collect();
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        // Build per-token LIKE patterns
        let patterns: Vec<String> = tokens.iter().map(|t| format!("%{}%", t)).collect();
        // For ORDER BY exact-name match, use the longest token
        let longest_token = tokens.iter().max_by_key(|t| t.len()).unwrap_or(&query);
        let longest_lower = longest_token.to_lowercase();
        let limit = limit as i64;

        let mut sql = String::from(
            "SELECT s.id, f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE ",
        );

        // Each token must match name OR qualname (AND across tokens)
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        for (i, pat) in patterns.iter().enumerate() {
            if i > 0 {
                sql.push_str(" AND ");
            }
            sql.push_str("(s.name LIKE ? OR s.qualname LIKE ?)");
            params.push(Box::new(pat.clone()));
            params.push(Box::new(pat.clone()));
        }

        sql.push_str(
            " AND s.graph_version = ? AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        params.push(Box::new(graph_version));
        params.push(Box::new(graph_version));

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
                params.push(Box::new(language.clone()));
            }
        }
        // Relevance-based ordering:
        // 1. Exact name match (longest token) first
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
        params.push(Box::new(longest_lower));
        params.push(Box::new(limit));

        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*param_refs, symbol_from_row)?;

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
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&pattern, &graph_version, &graph_version];
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
        sql.push_str(" ORDER BY s.name LIMIT ?");
        params.push(&limit);

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, symbol_from_row)?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
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
        sql.push_str(" LIMIT 1");
        self.read_conn()?
            .query_row(&sql, &*params, |row| row.get(0))
            .optional()
            .map_err(Into::into)
    }

    /// Fuzzy lookup for symbol IDs, handling short qualnames like
    /// "_svc.DeployAsync" or "helper::process"
    ///
    /// Strategy:
    /// 1. Try exact match first (fast path)
    /// 2. If the qualname carries a receiver segment (e.g. "connection" in "connection.Open"),
    ///    prefer candidates whose parent segment matches the receiver
    /// 3. Fall back to suffix match (`%.Open` / `%::Open`), preferring shortest qualname
    pub fn lookup_symbol_id_fuzzy(
        &self,
        target_qualname: &str,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Option<i64>> {
        use super::{fuzzy_qualname_patterns, qualname_receiver_segment, receiver_match_patterns};

        // Fast path: try exact match first
        if let Some(id) =
            self.lookup_symbol_id_filtered(target_qualname, languages, graph_version)?
        {
            return Ok(Some(id));
        }

        // Extract the trailing name and build suffix patterns for both '.' and '::'
        let (name, dot_pattern, colons_pattern) = fuzzy_qualname_patterns(target_qualname);

        // --- Receiver-segment preference pass ---
        // If the unresolved qualname carries a receiver (e.g. "connection" in "connection.Open"),
        // try to find a candidate whose parent segment matches that receiver before falling back
        // to the broad suffix match.
        if let Some(recv) = qualname_receiver_segment(target_qualname) {
            let recv_patterns = receiver_match_patterns(recv, name);
            let mut recv_sql = String::from(
                "SELECT s.id
                 FROM symbols s
                 JOIN files f ON s.file_id = f.id
                 WHERE s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
                   AND s.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                   AND (",
            );
            for (idx, _) in recv_patterns.iter().enumerate() {
                if idx > 0 {
                    recv_sql.push_str(" OR ");
                }
                recv_sql.push_str("s.qualname LIKE ? ESCAPE '\\'");
            }
            recv_sql.push(')');
            if let Some(languages) = languages
                && !languages.is_empty()
            {
                recv_sql.push_str(" AND f.language IN (");
                for (idx, _) in languages.iter().enumerate() {
                    if idx > 0 {
                        recv_sql.push(',');
                    }
                    recv_sql.push('?');
                }
                recv_sql.push(')');
            }
            recv_sql.push_str(" ORDER BY LENGTH(s.qualname) ASC LIMIT 1");

            let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> =
                vec![Box::new(graph_version), Box::new(graph_version)];
            for pat in &recv_patterns {
                params_vec.push(Box::new(pat.clone()));
            }
            if let Some(languages) = languages
                && !languages.is_empty()
            {
                for lang in languages {
                    params_vec.push(Box::new(lang.clone()));
                }
            }
            let param_refs: Vec<&dyn rusqlite::ToSql> =
                params_vec.iter().map(|p| p.as_ref()).collect();

            let conn = self.read_conn()?;
            let receiver_hit: Option<i64> = conn
                .query_row(&recv_sql, &*param_refs, |row| row.get(0))
                .optional()?;
            if receiver_hit.is_some() {
                return Ok(receiver_hit);
            }
        }

        // --- Suffix fallback ---
        // No receiver match found; fall back to the broad suffix pattern, preferring
        // the shortest qualname (least nesting = most specific in the absence of better signal).
        let mut sql = String::from(
            "SELECT s.id, s.qualname, LENGTH(s.qualname) as qn_len
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE (s.qualname = ? OR s.qualname LIKE ? OR s.qualname LIKE ?)
               AND s.kind IN ('method', 'function', 'class', 'interface', 'struct', 'property', 'enum', 'trait', 'type', 'record', 'service')
               AND s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );

        let mut params: Vec<&dyn rusqlite::ToSql> = vec![
            &name,
            &dot_pattern,
            &colons_pattern,
            &graph_version,
            &graph_version,
        ];

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

        // Prefer the shortest qualname (already ordered by qn_len ASC)
        Ok(candidates.first().map(|(id, _)| *id))
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
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.resolution_confidence
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE (e.source_symbol_id = ? OR e.target_symbol_id = ?)
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&id, &id, &graph_version, &graph_version];
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
        sql.push_str(" ORDER BY e.id");
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, edge_from_row)?;
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
        let broad_pattern = format!("%.{}", symbol_name);
        let exact = symbol_name.to_string();
        self.incoming_edges_by_target_patterns(
            &[broad_pattern],
            Some(&exact),
            kind,
            languages,
            graph_version,
        )
    }

    /// Find incoming edges preferring edges whose `target_qualname` receiver-segment matches the
    /// parent of the looked-up symbol (`symbol_qualname`).
    ///
    /// For example, given `symbol_qualname = "data.SqlConnection.Open"`:
    /// - The trailing name is `"Open"` and the receiver segment is `"SqlConnection"`.
    /// - First queries edges whose `target_qualname` matches `%SqlConnection.Open` or
    ///   `%SqlConnection::Open` (with `ESCAPE '\'`).
    /// - If those edges exist, returns them (receiver-preferred set).
    /// - If none are found, falls back to the broad `%.Open` pattern so that legitimate
    ///   bare-name callers are not lost.
    /// - If `symbol_qualname` has no receiver segment (bare name or top-level function),
    ///   returns the broad `%.name` set directly.
    pub fn incoming_edges_preferring_receiver(
        &self,
        symbol_qualname: &str,
        kind: &str,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        use super::{qualname_receiver_segment, qualname_trailing_name, receiver_match_patterns};

        let name = qualname_trailing_name(symbol_qualname);
        if name.is_empty() {
            // Degenerate qualname — fall through to broad
            let broad = format!("%.{}", symbol_qualname);
            return self.incoming_edges_by_target_patterns(
                &[broad],
                Some(symbol_qualname),
                kind,
                languages,
                graph_version,
            );
        }

        // Attempt receiver-segment preference when the qualname carries a parent.
        if let Some(receiver) = qualname_receiver_segment(symbol_qualname) {
            let recv_patterns = receiver_match_patterns(receiver, name);
            let preferred = self.incoming_edges_by_target_patterns(
                &recv_patterns,
                None, // LIKE patterns only — no bare-exact match on receiver path
                kind,
                languages,
                graph_version,
            )?;
            if !preferred.is_empty() {
                return Ok(preferred);
            }
            // Fall back to broad suffix match
        }

        let broad = format!("%.{}", name);
        let exact = name.to_string();
        self.incoming_edges_by_target_patterns(
            &[broad],
            Some(&exact),
            kind,
            languages,
            graph_version,
        )
    }

    /// Shared query helper for both `incoming_edges_by_qualname_pattern` and
    /// `incoming_edges_preferring_receiver`.
    ///
    /// Builds a query that matches edges where `target_qualname`:
    /// - LIKE any pattern in `like_patterns` (each pattern should already include `%` wildcards)
    /// - OR equals `exact_match` when `Some`
    ///
    /// All LIKE patterns use `ESCAPE '\'` so that identifiers containing `%`, `_`, or `\`
    /// are handled safely (patterns produced by [`receiver_match_patterns`] are already escaped).
    fn incoming_edges_by_target_patterns(
        &self,
        like_patterns: &[String],
        exact_match: Option<&str>,
        kind: &str,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        if like_patterns.is_empty() && exact_match.is_none() {
            return Ok(vec![]);
        }

        let mut sql = String::from(
            "SELECT e.id, f.path, e.kind, e.source_symbol_id, e.target_symbol_id,
                    e.target_qualname, e.detail, e.evidence_snippet,
                    e.evidence_start_line, e.evidence_end_line, e.confidence,
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.resolution_confidence
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE (",
        );

        let mut first = true;
        for _ in like_patterns {
            if !first {
                sql.push_str(" OR ");
            }
            sql.push_str("e.target_qualname LIKE ? ESCAPE '\\'");
            first = false;
        }
        if exact_match.is_some() {
            if !first {
                sql.push_str(" OR ");
            }
            sql.push_str("e.target_qualname = ?");
        }

        sql.push_str(
            ")
               AND e.kind = ?
               AND e.source_symbol_id IS NOT NULL
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );

        // Build owned params to avoid lifetime conflicts
        let mut owned: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        for pat in like_patterns {
            owned.push(Box::new(pat.clone()));
        }
        if let Some(ex) = exact_match {
            owned.push(Box::new(ex.to_string()));
        }
        owned.push(Box::new(kind.to_string()));
        owned.push(Box::new(graph_version));
        owned.push(Box::new(graph_version));

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
            for lang in languages {
                owned.push(Box::new(lang.clone()));
            }
        }

        sql.push_str(" ORDER BY e.id LIMIT 100");

        let params_ref: Vec<&dyn rusqlite::ToSql> = owned.iter().map(|b| b.as_ref()).collect();

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params_ref.as_slice(), edge_from_row)?;
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
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.resolution_confidence
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE e.target_qualname = ?
               AND e.kind IN ({kind_placeholders})
               AND e.source_symbol_id IS NOT NULL
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)"
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&target_qualname as &dyn rusqlite::ToSql];
        for kind in kinds {
            params.push(kind as &dyn rusqlite::ToSql);
        }
        params.push(&graph_version);
        params.push(&graph_version);
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
        sql.push_str(" ORDER BY e.id LIMIT 100");
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, edge_from_row)?;
        let mut edges = Vec::new();
        for row in rows {
            edges.push(row?);
        }
        Ok(edges)
    }

    /// Find unique source symbol IDs from edges targeting a config URI.
    /// If `kinds` is empty, searches all CONFIG edge kinds.
    pub fn source_symbols_for_config_uri(
        &self,
        uri: &str,
        kinds: &[&str],
        graph_version: i64,
    ) -> Result<Vec<i64>> {
        let default_kinds: &[&str] = &["CONFIG_SOURCE", "CONFIG_READ", "CONFIG_BIND"];
        let search_kinds = if kinds.is_empty() {
            default_kinds
        } else {
            kinds
        };
        let edges =
            self.edges_by_target_qualname_and_kinds(uri, search_kinds, None, graph_version)?;
        let mut seen = HashSet::new();
        let mut ids = Vec::new();
        for e in &edges {
            if let Some(id) = e.source_symbol_id
                && seen.insert(id)
            {
                ids.push(id);
            }
        }
        Ok(ids)
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
                    e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                    e.resolution_confidence
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
        sql.push_str(" ORDER BY e.id");

        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, edge_from_row)?;

        // Group edges by symbol ID
        let mut result: HashMap<i64, Vec<Edge>> = HashMap::new();
        for id in ids {
            result.insert(*id, Vec::new());
        }

        let mut seen_edge_ids = HashSet::new();
        for row in rows {
            let edge = row?;
            seen_edge_ids.insert(edge.id);
            // Add edge to both source and target symbol lists
            if let Some(source_id) = edge.source_symbol_id
                && ids.contains(&source_id)
            {
                result.entry(source_id).or_default().push(edge.clone());
            }
            if let Some(target_id) = edge.target_symbol_id
                && ids.contains(&target_id)
            {
                result.entry(target_id).or_default().push(edge.clone());
            }
        }

        // Second query: unresolved edges where target_qualname matches symbol names
        // This catches cross-file CALLS edges with short qualnames like "_svc.DeployAsync"
        let symbols_sql = format!(
            "SELECT id, name FROM symbols WHERE id IN ({}) AND graph_version = ?",
            placeholders
        );
        let mut symbols_params: Vec<&dyn rusqlite::ToSql> =
            ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
        symbols_params.push(&graph_version);

        let mut stmt = conn.prepare(&symbols_sql)?;
        let mut symbol_rows = stmt.query(&*symbols_params)?;
        let mut symbol_names: HashMap<String, i64> = HashMap::new();
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
                        e.graph_version, e.commit_sha, e.trace_id, e.span_id, e.event_ts,
                        e.resolution_confidence
                 FROM edges e
                 JOIN files f ON e.file_id = f.id
                 WHERE e.target_symbol_id IS NULL
                   AND e.graph_version = ?
                   AND (f.deleted_version IS NULL OR f.deleted_version > ?)
                   AND (",
            );

            let mut unresolved_params: Vec<&dyn rusqlite::ToSql> =
                vec![&graph_version, &graph_version];

            for (idx, pattern) in patterns.iter().enumerate() {
                if idx > 0 {
                    unresolved_sql.push_str(" OR ");
                }
                unresolved_sql.push_str("e.target_qualname LIKE ?");
                unresolved_params.push(pattern as &dyn rusqlite::ToSql);
            }
            unresolved_sql.push(')');

            if let Some(languages) = languages
                && !languages.is_empty()
            {
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
            unresolved_sql.push_str(" ORDER BY e.id");

            let mut stmt = conn.prepare(&unresolved_sql)?;
            let rows = stmt.query_map(&*unresolved_params, edge_from_row)?;

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
        sql.push_str(" ORDER BY s.id");
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(&*params, symbol_from_row)?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
}
