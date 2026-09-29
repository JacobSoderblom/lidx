use super::{Db, edge_from_row, symbol_from_row};
use crate::model::{Edge, EdgeSnapshotRow, Symbol};
use anyhow::Result;
use rusqlite::OptionalExtension;
use std::collections::{HashMap, HashSet};

impl Db {
    /// Types that `type_id` IMPLEMENTS (outgoing edges).
    ///
    /// A bare-name IMPLEMENTS target ("IPublisher") can bind to the file's
    /// same-named `module` symbol rather than the type (C# file modules
    /// carry the bare file name as qualname), so an edge target that is a
    /// module/namespace stands for every type sharing its name; callers
    /// confirm a match by an exact member qualname.
    pub fn implemented_types(&self, type_id: i64, graph_version: i64) -> Result<Vec<i64>> {
        self.type_ids(
            "SELECT DISTINCT t.id
             FROM edges e
             JOIN symbols t0 ON t0.id = e.target_symbol_id
             JOIN symbols t ON t.graph_version = ?1
                           AND t.kind IN ('interface', 'class', 'struct', 'trait')
                           AND (t.id = t0.id
                                OR (t0.kind IN ('module', 'namespace') AND t.name = t0.name))
             WHERE e.source_symbol_id = ?2 AND e.kind = 'IMPLEMENTS' AND e.graph_version = ?1
             ORDER BY t.id",
            type_id,
            graph_version,
        )
    }

    /// Types that IMPLEMENT `type_id` (incoming edges), with the same
    /// module-binding tolerance as [`Db::implemented_types`].
    pub fn implementing_types(&self, type_id: i64, graph_version: i64) -> Result<Vec<i64>> {
        self.type_ids(
            "SELECT DISTINCT e.source_symbol_id
             FROM symbols p
             JOIN edges e ON e.kind = 'IMPLEMENTS' AND e.graph_version = ?1
                         AND e.source_symbol_id IS NOT NULL
             JOIN symbols t0 ON t0.id = e.target_symbol_id
                            AND (t0.id = p.id
                                 OR (t0.kind IN ('module', 'namespace') AND t0.name = p.name))
             WHERE p.id = ?2
             ORDER BY e.source_symbol_id",
            type_id,
            graph_version,
        )
    }

    fn type_ids(&self, sql: &str, type_id: i64, graph_version: i64) -> Result<Vec<i64>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(sql)?;
        let rows = stmt.query_map(rusqlite::params![graph_version, type_id], |r| {
            r.get::<_, i64>(0)
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    /// The one lookup behind interface dispatch (issue #122): a call through
    /// an interface-typed receiver only ever resolves to the *interface*
    /// method, so the implementing method looks uncalled. Given a method,
    /// returns the same-named methods on the interfaces its class
    /// `IMPLEMENTS` (`interface_methods`), and the same-named methods on the
    /// classes that `IMPLEMENTS` its interface (`impl_methods`). Empty for
    /// non-methods and for classes/interfaces with no resolved IMPLEMENTS
    /// edges. Language-agnostic: keyed on the qualname shape
    /// `<parent><sep><name>` only.
    pub fn dispatch_peers(&self, symbol_id: i64, graph_version: i64) -> Result<DispatchPeers> {
        let mut peers = DispatchPeers::default();
        let Some(sym) = self.get_symbol_by_id(symbol_id)? else {
            return Ok(peers);
        };
        if sym.kind != "method" {
            return Ok(peers);
        }
        let Some(head) = sym.qualname.strip_suffix(sym.name.as_str()) else {
            return Ok(peers);
        };
        let (parent, sep) = if let Some(p) = head.strip_suffix("::") {
            (p, "::")
        } else if let Some(p) = head.strip_suffix('.') {
            (p, ".")
        } else {
            return Ok(peers);
        };
        let parent_id = self.lookup_symbol_id(parent, graph_version)?;
        let Some(parent_id) = parent_id else {
            return Ok(peers);
        };
        let sibling = |type_ids: Vec<i64>, out: &mut Vec<i64>| -> Result<()> {
            for type_id in type_ids {
                let Some(t) = self.get_symbol_by_id(type_id)? else {
                    continue;
                };
                let qn = format!("{}{sep}{}", t.qualname, sym.name);
                if let Some(id) = self.lookup_symbol_id(&qn, graph_version)?
                    && !out.contains(&id)
                {
                    out.push(id);
                }
            }
            Ok(())
        };
        sibling(
            self.implemented_types(parent_id, graph_version)?,
            &mut peers.interface_methods,
        )?;
        sibling(
            self.implementing_types(parent_id, graph_version)?,
            &mut peers.impl_methods,
        )?;
        Ok(peers)
    }
}

impl Db {
    /// [`Db::dispatch_peers`] as synthetic CALLS edges (interface method ->
    /// implementing method, `resolution_kind = "interface_dispatch"`, id 0)
    /// so graph traversals (trace_flow, analyze_impact) can walk dynamic
    /// dispatch with their ordinary edge handling: downstream from the
    /// interface method reaches the impls, upstream from an impl reaches the
    /// interface method and, through its own real edges, its callers.
    pub fn dispatch_edges(&self, symbol_id: i64, graph_version: i64) -> Result<Vec<Edge>> {
        let peers = self.dispatch_peers(symbol_id, graph_version)?;
        let mut edges = Vec::new();
        let pairs = peers
            .interface_methods
            .iter()
            .map(|&i| (i, symbol_id, i))
            .chain(peers.impl_methods.iter().map(|&m| (symbol_id, m, m)));
        for (source, target, peer) in pairs {
            let Some(peer_sym) = self.get_symbol_by_id(peer)? else {
                continue;
            };
            edges.push(Edge {
                id: 0,
                file_path: peer_sym.file_path,
                kind: "CALLS".to_string(),
                source_symbol_id: Some(source),
                target_symbol_id: Some(target),
                target_qualname: None,
                detail: Some("interface dispatch".to_string()),
                evidence_snippet: None,
                evidence_start_line: None,
                evidence_end_line: None,
                confidence: None,
                resolution_kind: Some("interface_dispatch".to_string()),
                graph_version,
                commit_sha: None,
                trace_id: None,
                span_id: None,
                event_ts: None,
            });
        }
        Ok(edges)
    }
}

/// Result of [`Db::dispatch_peers`].
#[derive(Debug, Default, Clone)]
pub struct DispatchPeers {
    /// Same-named methods on interfaces the method's class implements.
    pub interface_methods: Vec<i64>,
    /// Same-named methods on classes implementing the method's interface.
    pub impl_methods: Vec<i64>,
}

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
             CASE WHEN s.kind IN ('class','function','method','struct','interface','enum','trait','service','message') THEN 0 \
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
                    e.resolution_kind
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

    /// Find edges by exact target_qualname match and edge kind filter.
    /// Used for traversal bridging: given a channel/route qualname, find all
    /// edges pointing at it with complementary kinds.
    ///
    /// RPC paths additionally bind by `service/method` suffix when a side
    /// guessed the proto package wrong (see `resolve_rpc_route`).
    pub fn edges_by_target_qualname_and_kinds(
        &self,
        target_qualname: &str,
        kinds: &[&str],
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<Edge>> {
        let is_rpc = kinds.iter().any(|k| matches!(*k, "RPC_CALL" | "RPC_IMPL"));
        if !is_rpc || !target_qualname.starts_with('/') {
            return self.edges_by_exact_target(
                target_qualname,
                target_qualname,
                kinds,
                languages,
                graph_version,
            );
        }
        // Our own path may be a wrong guess: bind it to the one real route.
        let own = self.resolve_rpc_route(target_qualname, graph_version)?;
        let route = own.as_deref().unwrap_or(target_qualname);
        let mut edges =
            self.edges_by_exact_target(route, route, kinds, languages, graph_version)?;
        // The other side may be the wrong guess: pull in its edges whose
        // guessed path resolves to our route.
        if kinds.contains(&"RPC_CALL") && own.is_none() {
            let (_, svc_method) = rpc_split(target_qualname);
            let conn = self.read_conn()?;
            let mut stmt = conn.prepare(
                "SELECT DISTINCT target_qualname FROM edges
                 WHERE kind = 'RPC_CALL' AND graph_version = ?1
                   AND target_qualname != ?2
                   AND (target_qualname = '/' || ?3
                        OR substr(target_qualname, -length(?3) - 1) = '.' || ?3)",
            )?;
            let guesses: Vec<String> = stmt
                .query_map(
                    rusqlite::params![graph_version, target_qualname, svc_method],
                    |r| r.get(0),
                )?
                .collect::<rusqlite::Result<_>>()?;
            for guess in guesses {
                if self.resolve_rpc_route(&guess, graph_version)?.as_deref()
                    == Some(target_qualname)
                {
                    edges.extend(self.edges_by_exact_target(
                        &guess,
                        target_qualname,
                        &["RPC_CALL"],
                        languages,
                        graph_version,
                    )?);
                }
            }
        }
        Ok(edges)
    }

    /// Bind a guessed RPC path (`/pkg.service/method`, package possibly wrong
    /// or missing) to a real `.proto` RPC_ROUTE path. `None` when the path
    /// already has an exact route, or no single route can be chosen: one
    /// `service/method` suffix match binds; several are narrowed to those
    /// whose package agrees with the guessed package (one a dotted suffix
    /// of the other); still ambiguous means unbound.
    fn resolve_rpc_route(&self, guess: &str, graph_version: i64) -> Result<Option<String>> {
        let (guess_pkg, svc_method) = rpc_split(guess);
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT target_qualname FROM edges
             WHERE kind = 'RPC_ROUTE' AND graph_version = ?1
               AND (target_qualname = '/' || ?2
                    OR substr(target_qualname, -length(?2) - 1) = '.' || ?2)",
        )?;
        let routes: Vec<String> = stmt
            .query_map(rusqlite::params![graph_version, svc_method], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        if routes.iter().any(|r| r == guess) {
            return Ok(None);
        }
        if routes.len() > 1 && !guess_pkg.is_empty() {
            let agree: Vec<&String> = routes
                .iter()
                .filter(|r| {
                    let (pkg, _) = rpc_split(r);
                    pkg == guess_pkg
                        || pkg.ends_with(&format!(".{guess_pkg}"))
                        || guess_pkg.ends_with(&format!(".{pkg}"))
                })
                .collect();
            if let [only] = agree.as_slice() {
                return Ok(Some((*only).clone()));
            }
        }
        Ok(match routes.as_slice() {
            [only] => Some(only.clone()),
            _ => None,
        })
    }

    /// `route` is the RPC_ROUTE path that must back RPC edges (the target
    /// itself, or the real route a guessed target resolved to).
    fn edges_by_exact_target(
        &self,
        target_qualname: &str,
        route: &str,
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
                    e.resolution_kind
             FROM edges e
             JOIN files f ON e.file_id = f.id
             WHERE e.target_qualname = ?
               AND e.kind IN ({kind_placeholders})
               AND e.source_symbol_id IS NOT NULL
               AND e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
               AND (e.kind NOT IN ('RPC_CALL', 'RPC_IMPL')
                    OR EXISTS (SELECT 1 FROM edges r
                               WHERE r.target_qualname = ?
                                 AND r.kind = 'RPC_ROUTE' AND r.graph_version = ?)
                    OR NOT EXISTS (SELECT 1 FROM edges r
                                   WHERE r.kind = 'RPC_ROUTE' AND r.graph_version = ?))"
        );
        // ponytail: RPC_CALL and RPC_IMPL both fan out one edge per
        // *guessed* proto package (bare `using`s), so two wrong guesses can
        // share a path and bridge unrelated services. Only bridge on a path
        // a real `.proto` RPC_ROUTE backs; repos with no RPC_ROUTE at all
        // (protos live elsewhere) keep the unguarded behaviour. Ceiling: a
        // route whose .proto isn't indexed is still exposed to that noise.
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&target_qualname as &dyn rusqlite::ToSql];
        for kind in kinds {
            params.push(kind as &dyn rusqlite::ToSql);
        }
        params.push(&graph_version);
        params.push(&graph_version);
        params.push(&route as &dyn rusqlite::ToSql);
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
                    e.resolution_kind
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

        for row in rows {
            let edge = row?;
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

        // A second query used to widen this result set by matching
        // `target_symbol_id IS NULL` edges against symbol *names* (via a
        // `LIKE '%.Name'` suffix pattern) — i.e. it re-attributed edges the
        // write path had deliberately left unresolved. That's exactly the
        // guess the write path already refused to make: SQLite's LIKE is
        // case-insensitive for ASCII (`'value.Trim' LIKE '%.trim'` is true)
        // and the pattern carried no language scoping, so it matched same-
        // named methods across unrelated classes and even unrelated
        // languages (see the C#-`Trim`-to-Python-`trim` false callee).
        // `target_symbol_id IS NULL` now means "could not be attributed" —
        // the read path must not invent one. A target that *can* be bound
        // legitimately (e.g. an exact qualname match) belongs in the write
        // path (`insert_edges`) or its repair passes (`db::resolver`'s
        // `retry_unresolved_references`/`reconcile_unresolved_reference_store`),
        // not here.
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

    /// Read every edge in `graph_version` as a normalized
    /// `(source qualname, kind, target qualname | None, resolution kind)`
    /// row, joining `source_symbol_id`/`target_symbol_id` to their symbols'
    /// *current* qualnames rather than the edge's raw stored
    /// `target_qualname` text. Issue #79: a pending (non-Bridge-Edge-kind)
    /// `unresolved_references` row -- one with no edge at all -- is unioned
    /// in too, as `target qualname = None`/`resolution kind = None` (i.e.
    /// `UNRESOLVED`, same as a Bridge Edge kind's still-NULL-target edge
    /// already renders via the first half of this query), so the scoreboard
    /// keeps seeing every unresolved reference regardless of which of the
    /// two shapes holds it.
    ///
    /// Test support for the golden-corpus correctness scoreboard (see
    /// `tests/common/golden.rs`): the one seam a test needs to compare the
    /// graph against an expected-edges fixture without touching SQL or
    /// resolver internals directly. A reference with no resolved source
    /// symbol (file-level edges such as `IMPORTS`/`MODULE_FILE`, or a
    /// pending row whose own caller never resolved) is omitted — the
    /// scoreboard only covers edges attributable to a real symbol.
    pub fn edges_snapshot(&self, graph_version: i64) -> Result<Vec<EdgeSnapshotRow>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT src.qualname, e.kind, tgt.qualname, e.resolution_kind
             FROM edges e
             JOIN files f ON e.file_id = f.id
             JOIN symbols src ON e.source_symbol_id = src.id
             LEFT JOIN symbols tgt ON e.target_symbol_id = tgt.id
             WHERE e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)

             UNION ALL

             SELECT src.qualname, ur.edge_kind, NULL, NULL
             FROM unresolved_references ur
             JOIN files f ON ur.file_id = f.id
             JOIN symbols src ON ur.source_symbol_id = src.id
             WHERE ur.edge_id IS NULL
               AND ur.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        )?;
        let rows = stmt.query_map(
            rusqlite::params![graph_version, graph_version, graph_version, graph_version],
            |row| {
                Ok(EdgeSnapshotRow {
                    source_qualname: row.get(0)?,
                    kind: row.get(1)?,
                    target_qualname: row.get(2)?,
                    resolution_kind: row.get(3)?,
                })
            },
        )?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        results.sort();
        Ok(results)
    }

    /// `edges.resolution_kind` for a batch of edge ids, keyed by edge id.
    /// Since issue #62, `Edge::resolution_kind` (via `edge_from_row`) also
    /// carries this column for any caller that already has full `Edge`
    /// values in hand -- prefer reading `edge.resolution_kind` directly in
    /// that case, as `trace_flow`'s BFS (`traversal.rs`) and
    /// `analyze_direct_impact`'s BFS (`impact/layers/direct.rs`) now do for
    /// their per-node `exclude_resolution_kinds` filter (issue #81). This
    /// batch, id-keyed form stays for the one remaining case where a caller
    /// only has bare edge ids: those same two BFS functions' post-traversal
    /// `traversed_heuristic_kind` summary, which checks every edge that
    /// actually produced a hop/neighbor in one query rather than loading a
    /// full `Edge` per id just to read one field. An edge id absent from the
    /// returned map has no resolution kind at all -- a Bridge Edge kind
    /// (its target is a cross-process join key, resolved separately from
    /// the name tiers) or any edge kind the resolver never labels.
    pub fn edge_resolution_kinds(&self, edge_ids: &[i64]) -> Result<HashMap<i64, String>> {
        if edge_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut placeholders = String::new();
        for (idx, _) in edge_ids.iter().enumerate() {
            if idx > 0 {
                placeholders.push(',');
            }
            placeholders.push('?');
        }
        let sql = format!(
            "SELECT id, resolution_kind FROM edges \
             WHERE id IN ({placeholders}) AND resolution_kind IS NOT NULL"
        );
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::ToSql> = edge_ids
            .iter()
            .map(|id| id as &dyn rusqlite::ToSql)
            .collect();
        let rows = stmt.query_map(&*params, |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut map = HashMap::new();
        for row in rows {
            let (id, kind) = row?;
            map.insert(id, kind);
        }
        Ok(map)
    }
}

/// Split `/pkg.Service/method` into (`pkg`, `service/method`); `pkg` is empty
/// when the path has none.
fn rpc_split(path: &str) -> (&str, &str) {
    let path = path.trim_start_matches('/');
    let svc_path = path.split_once('/').map_or(path, |(svc, _)| svc);
    match svc_path.rfind('.') {
        Some(i) => (&path[..i], &path[i + 1..]),
        None => ("", path),
    }
}
