use super::{Db, DbDigest, ModuleSummaryEntry, SymbolRefRecord, TableDigest, append_path_filters};
use crate::model::RepoOverview;
use anyhow::Result;
use blake3::Hasher;
use rusqlite::{Connection, params};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

impl Db {
    pub fn repo_overview(
        &self,
        repo_root: PathBuf,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<RepoOverview> {
        let last_indexed = self.get_meta_i64("last_indexed")?;
        let commit_sha = self.graph_version_commit(graph_version)?;

        let conn = self.read_conn()?;
        let files = count_files_for_version(&conn, languages, graph_version)?;
        let symbols = count_symbols_for_version(&conn, languages, graph_version)?;
        let edges = count_edges_for_version(&conn, languages, graph_version)?;

        Ok(RepoOverview {
            repo_root: repo_root.to_string_lossy().to_string(),
            files,
            symbols,
            edges,
            last_indexed,
            graph_version: Some(graph_version),
            commit_sha,
        })
    }

    pub fn list_languages(&self, graph_version: i64) -> Result<Vec<String>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT DISTINCT language
             FROM files
             WHERE deleted_version IS NULL OR deleted_version > ?
             ORDER BY language",
        )?;
        let rows = stmt.query_map(params![graph_version], |row| row.get(0))?;
        let mut languages = Vec::new();
        for row in rows {
            languages.push(row?);
        }
        Ok(languages)
    }

    pub fn list_symbol_refs(&self, graph_version: i64) -> Result<Vec<SymbolRefRecord>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT s.id, s.name, s.qualname, s.kind, f.language
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)",
        )?;
        let rows = stmt.query_map(params![graph_version, graph_version], |row| {
            Ok(SymbolRefRecord {
                id: row.get(0)?,
                name: row.get(1)?,
                qualname: row.get(2)?,
                kind: row.get(3)?,
                language: row.get(4)?,
            })
        })?;
        let mut records = Vec::new();
        for row in rows {
            records.push(row?);
        }
        Ok(records)
    }

    pub fn symbol_map_for_file(
        &self,
        file_id: i64,
        graph_version: i64,
    ) -> Result<HashMap<String, i64>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, qualname
             FROM symbols
             WHERE file_id = ? AND graph_version = ?",
        )?;
        let rows = stmt.query_map(params![file_id, graph_version], |row| {
            let id: i64 = row.get(0)?;
            let qualname: String = row.get(1)?;
            Ok((qualname, id))
        })?;
        let mut map = HashMap::new();
        for row in rows {
            let (qualname, id) = row?;
            map.insert(qualname, id);
        }
        Ok(map)
    }

    pub fn digest(&self) -> Result<DbDigest> {
        Ok(DbDigest {
            files: self.digest_files()?,
            symbols: self.digest_symbols()?,
            edges: self.digest_edges()?,
        })
    }

    fn digest_files(&self) -> Result<TableDigest> {
        let graph_version = self.current_graph_version()?;
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT path, hash, language, size, deleted_version
             FROM files
             WHERE deleted_version IS NULL OR deleted_version > ?
             ORDER BY path",
        )?;
        let rows = stmt.query_map(params![graph_version], |row| {
            let path: String = row.get(0)?;
            let hash: String = row.get(1)?;
            let language: String = row.get(2)?;
            let size: i64 = row.get(3)?;
            let deleted_version: Option<i64> = row.get(4)?;
            Ok(json!([path, hash, language, size, deleted_version]).to_string())
        })?;
        digest_rows(rows)
    }

    fn digest_symbols(&self) -> Result<TableDigest> {
        let graph_version = self.current_graph_version()?;
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT f.path, s.kind, s.name, s.qualname, s.start_line, s.start_col,
                    s.end_line, s.end_col, s.start_byte, s.end_byte, s.signature, s.docstring,
                    s.graph_version, s.commit_sha, s.stable_id
             FROM symbols s
             JOIN files f ON s.file_id = f.id
             WHERE s.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
             ORDER BY f.path, s.qualname, s.kind, s.start_line, s.start_col, s.end_line, s.end_col",
        )?;
        let rows = stmt.query_map(params![graph_version, graph_version], |row| {
            let path: String = row.get(0)?;
            let kind: String = row.get(1)?;
            let name: String = row.get(2)?;
            let qualname: String = row.get(3)?;
            let start_line: i64 = row.get(4)?;
            let start_col: i64 = row.get(5)?;
            let end_line: i64 = row.get(6)?;
            let end_col: i64 = row.get(7)?;
            let start_byte: i64 = row.get(8)?;
            let end_byte: i64 = row.get(9)?;
            let signature: Option<String> = row.get(10)?;
            let docstring: Option<String> = row.get(11)?;
            let row_graph_version: i64 = row.get(12)?;
            let commit_sha: Option<String> = row.get(13)?;
            let stable_id: Option<String> = row.get(14)?;
            Ok(json!([
                path,
                kind,
                name,
                qualname,
                start_line,
                start_col,
                end_line,
                end_col,
                start_byte,
                end_byte,
                signature,
                docstring,
                row_graph_version,
                commit_sha,
                stable_id
            ])
            .to_string())
        })?;
        digest_rows(rows)
    }

    fn digest_edges(&self) -> Result<TableDigest> {
        let graph_version = self.current_graph_version()?;
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT f.path,
                    e.kind,
                    src.qualname,
                    COALESCE(tgt.qualname, e.target_qualname),
                    e.detail,
                    e.evidence_snippet,
                    e.evidence_start_line,
                    e.evidence_end_line,
                    e.confidence,
                    e.graph_version,
                    e.commit_sha,
                    e.trace_id,
                    e.span_id,
                    e.event_ts
             FROM edges e
             JOIN files f ON e.file_id = f.id
             LEFT JOIN symbols src ON e.source_symbol_id = src.id
             LEFT JOIN symbols tgt ON e.target_symbol_id = tgt.id
             WHERE e.graph_version = ?
               AND (f.deleted_version IS NULL OR f.deleted_version > ?)
             ORDER BY f.path,
                      e.kind,
                      COALESCE(src.qualname, ''),
                      COALESCE(tgt.qualname, e.target_qualname, ''),
                      COALESCE(e.detail, ''),
                      COALESCE(e.evidence_snippet, '')",
        )?;
        let rows = stmt.query_map(params![graph_version, graph_version], |row| {
            let path: String = row.get(0)?;
            let kind: String = row.get(1)?;
            let source: Option<String> = row.get(2)?;
            let target: Option<String> = row.get(3)?;
            let detail: Option<String> = row.get(4)?;
            let evidence_snippet: Option<String> = row.get(5)?;
            let evidence_start_line: Option<i64> = row.get(6)?;
            let evidence_end_line: Option<i64> = row.get(7)?;
            let confidence: Option<f64> = row.get(8)?;
            let row_graph_version: i64 = row.get(9)?;
            let commit_sha: Option<String> = row.get(10)?;
            let trace_id: Option<String> = row.get(11)?;
            let span_id: Option<String> = row.get(12)?;
            let event_ts: Option<i64> = row.get(13)?;
            Ok(json!([
                path,
                kind,
                source,
                target,
                detail,
                evidence_snippet,
                evidence_start_line,
                evidence_end_line,
                confidence,
                row_graph_version,
                commit_sha,
                trace_id,
                span_id,
                event_ts
            ])
            .to_string())
        })?;
        digest_rows(rows)
    }

    /// Get module-level aggregation: file counts and symbol counts per directory prefix
    pub fn module_summary(
        &self,
        depth: usize,
        languages: Option<&[String]>,
        paths: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<ModuleSummaryEntry>> {
        let conn = self.read_conn()?;

        // Query all files with their symbol counts
        let mut sql = String::from(
            "SELECT f.path, f.language, COUNT(s.id) as sym_count
             FROM files f
             LEFT JOIN symbols s ON s.file_id = f.id AND s.graph_version = ?
             WHERE (f.deleted_version IS NULL OR f.deleted_version > ?)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version, &graph_version];

        // Add language filter
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

        // Add path filter
        let mut path_params = Vec::new();
        append_path_filters(&mut sql, &mut params, &mut path_params, paths, "f");

        sql.push_str(" GROUP BY f.id");

        let mut stmt = conn.prepare(&sql)?;
        let rows: Vec<(String, String, i64)> = stmt
            .query_map(&*params, |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .filter_map(|r| r.ok())
            .collect();

        // Group by module prefix at given depth
        let mut modules: HashMap<String, (usize, usize, HashSet<String>)> = HashMap::new();

        for (path, lang, sym_count) in &rows {
            let prefix = module_prefix(path, depth);
            let entry = modules.entry(prefix).or_insert((0, 0, HashSet::new()));
            entry.0 += 1;
            entry.1 += *sym_count as usize;
            entry.2.insert(lang.clone());
        }

        let mut result: Vec<ModuleSummaryEntry> = modules
            .into_iter()
            .map(|(path, (fc, sc, langs))| ModuleSummaryEntry {
                path,
                file_count: fc,
                symbol_count: sc,
                languages: langs.into_iter().collect(),
            })
            .collect();
        result.sort_by_key(|x| std::cmp::Reverse(x.symbol_count));

        Ok(result)
    }

    /// Get inter-module edge counts (calls and imports)
    pub fn module_edges(
        &self,
        depth: usize,
        languages: Option<&[String]>,
        graph_version: i64,
    ) -> Result<Vec<(String, String, usize, usize)>> {
        let conn = self.read_conn()?;

        // Query all CALLS, IMPORTS, and XREF edges with source and target file paths
        let mut sql = String::from(
            "SELECT e.kind, src_f.path as src_path, tgt_f.path as tgt_path
             FROM edges e
             JOIN symbols src_s ON e.source_symbol_id = src_s.id
             JOIN files src_f ON src_s.file_id = src_f.id
             LEFT JOIN symbols tgt_s ON e.target_symbol_id = tgt_s.id
             LEFT JOIN files tgt_f ON tgt_s.file_id = tgt_f.id
             WHERE e.kind IN ('CALLS', 'IMPORTS', 'XREF')
               AND e.graph_version = ?
               AND (src_f.deleted_version IS NULL OR src_f.deleted_version > ?)
               AND (tgt_f.deleted_version IS NULL OR tgt_f.deleted_version > ? OR tgt_f.id IS NULL)",
        );
        let mut params: Vec<&dyn rusqlite::ToSql> =
            vec![&graph_version, &graph_version, &graph_version];

        // Add language filter
        if let Some(languages) = languages
            && !languages.is_empty()
        {
            sql.push_str(" AND src_f.language IN (");
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

        let mut stmt = conn.prepare(&sql)?;
        let rows: Vec<(String, String, Option<String>)> = stmt
            .query_map(&*params, |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .filter_map(|r| r.ok())
            .collect();

        // Group by source module -> target module
        let mut edge_map: HashMap<(String, String), (usize, usize)> = HashMap::new();

        for (kind, src_path, tgt_path_opt) in &rows {
            let src_module = module_prefix(src_path, depth);

            if let Some(tgt_path) = tgt_path_opt {
                let tgt_module = module_prefix(tgt_path, depth);

                if src_module == tgt_module {
                    continue;
                }

                let entry = edge_map.entry((src_module, tgt_module)).or_insert((0, 0));

                if kind == "CALLS" || kind == "XREF" {
                    entry.0 += 1;
                } else if kind == "IMPORTS" {
                    entry.1 += 1;
                }
            }
        }

        let mut result: Vec<_> = edge_map
            .into_iter()
            .map(|((src, tgt), (calls, imports))| (src, tgt, calls, imports))
            .collect();
        result.sort_by_key(|x| std::cmp::Reverse(x.2 + x.3)); // Sort by total edge count desc

        Ok(result)
    }
}

/// Extract a directory prefix from `path` at the given `depth`.
///
/// Paths deeper than `depth` are truncated (e.g. `"a/b/c.rs"` at depth 1
/// becomes `"a/"`). Paths at or below `depth` use their parent directory,
/// and root-level files map to `"."`.
fn module_prefix(path: &str, depth: usize) -> String {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() > depth {
        parts[..depth].join("/") + "/"
    } else if parts.len() > 1 {
        parts[..parts.len() - 1].join("/") + "/"
    } else {
        ".".to_string()
    }
}

fn digest_rows<I>(rows: I) -> Result<TableDigest>
where
    I: Iterator<Item = rusqlite::Result<String>>,
{
    let mut hasher = Hasher::new();
    let mut count = 0;
    for row in rows {
        let row = row?;
        hasher.update(row.as_bytes());
        hasher.update(b"\n");
        count += 1;
    }
    Ok(TableDigest {
        rows: count,
        hash: hasher.finalize().to_hex().to_string(),
    })
}

fn count_files_for_version(
    conn: &Connection,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<i64> {
    let mut sql = String::from(
        "SELECT COUNT(*)
         FROM files f
         WHERE (f.deleted_version IS NULL OR f.deleted_version > ?)",
    );
    let mut params: Vec<&dyn rusqlite::ToSql> = vec![&graph_version];
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
    let count: i64 = conn.query_row(&sql, &*params, |row| row.get(0))?;
    Ok(count)
}

fn count_symbols_for_version(
    conn: &Connection,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<i64> {
    let mut sql = String::from(
        "SELECT COUNT(*)
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
    let count: i64 = conn.query_row(&sql, &*params, |row| row.get(0))?;
    Ok(count)
}

fn count_edges_for_version(
    conn: &Connection,
    languages: Option<&[String]>,
    graph_version: i64,
) -> Result<i64> {
    let mut sql = String::from(
        "SELECT COUNT(*)
         FROM edges e
         JOIN files f ON e.file_id = f.id
         WHERE e.graph_version = ?
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
    let count: i64 = conn.query_row(&sql, &*params, |row| row.get(0))?;
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::indexer::extract::{EdgeInput, SymbolInput};
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
            name: qualname.split('.').last().unwrap_or(qualname).to_string(),
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

    // ---- module_prefix ----

    #[test]
    fn module_prefix_depth_1() {
        assert_eq!(module_prefix("src/db/overview.rs", 1), "src/");
    }

    #[test]
    fn module_prefix_depth_2() {
        assert_eq!(module_prefix("src/db/overview.rs", 2), "src/db/");
    }

    #[test]
    fn module_prefix_depth_exceeds_parts() {
        assert_eq!(module_prefix("src/db/overview.rs", 5), "src/db/");
    }

    #[test]
    fn module_prefix_root_level_file() {
        // Single-part path at depth 1: 1 > 1 is false, falls to else; 1 > 1 is false -> "."
        assert_eq!(module_prefix("main.rs", 1), ".");
        // Single-part path at depth 0: 1 > 0 is true -> parts[..0] = "/" (same as multi-part)
        assert_eq!(module_prefix("main.rs", 0), "/");
    }

    #[test]
    fn module_prefix_depth_zero_multi_part() {
        assert_eq!(module_prefix("src/db/overview.rs", 0), "/");
    }

    #[test]
    fn module_prefix_single_dir_at_boundary() {
        assert_eq!(module_prefix("src/main.rs", 1), "src/");
        assert_eq!(module_prefix("src/main.rs", 2), "src/");
    }

    // ---- digest_rows ----

    #[test]
    fn digest_rows_empty() {
        let rows: Vec<rusqlite::Result<String>> = vec![];
        let result = digest_rows(rows.into_iter()).unwrap();
        assert_eq!(result.rows, 0);
        assert!(!result.hash.is_empty());
    }

    #[test]
    fn digest_rows_deterministic() {
        let rows1 = vec![Ok("a".to_string()), Ok("b".to_string())];
        let rows2 = vec![Ok("a".to_string()), Ok("b".to_string())];
        let d1 = digest_rows(rows1.into_iter()).unwrap();
        let d2 = digest_rows(rows2.into_iter()).unwrap();
        assert_eq!(d1, d2);
    }

    #[test]
    fn digest_rows_order_matters() {
        let rows1 = vec![Ok("a".to_string()), Ok("b".to_string())];
        let rows2 = vec![Ok("b".to_string()), Ok("a".to_string())];
        let d1 = digest_rows(rows1.into_iter()).unwrap();
        let d2 = digest_rows(rows2.into_iter()).unwrap();
        assert_ne!(d1.hash, d2.hash);
    }

    #[test]
    fn digest_rows_propagates_error() {
        let rows = vec![
            Ok("a".to_string()),
            Err(rusqlite::Error::InvalidColumnType(
                0,
                "x".into(),
                rusqlite::types::Type::Null,
            )),
        ];
        assert!(digest_rows(rows.into_iter()).is_err());
    }

    // ---- list_languages ----

    #[test]
    fn list_languages_empty_db() {
        let (db, _temp) = create_test_db();
        assert!(db.list_languages(1).unwrap().is_empty());
    }

    #[test]
    fn list_languages_distinct_sorted() {
        let (db, _temp) = create_test_db();
        db.upsert_file("a.py", "h1", "python", 10, 0).unwrap();
        db.upsert_file("b.rs", "h2", "rust", 20, 0).unwrap();
        db.upsert_file("c.py", "h3", "python", 15, 0).unwrap();
        assert_eq!(db.list_languages(1).unwrap(), vec!["python", "rust"]);
    }

    #[test]
    fn list_languages_excludes_deleted() {
        let (db, _temp) = create_test_db();
        db.upsert_file("a.py", "h1", "python", 10, 0).unwrap();
        db.upsert_file("b.go", "h2", "go", 20, 0).unwrap();
        db.mark_file_deleted("b.go", 1).unwrap();
        assert_eq!(db.list_languages(1).unwrap(), vec!["python"]);
    }

    // ---- list_symbol_refs ----

    #[test]
    fn list_symbol_refs_empty_db() {
        let (db, _temp) = create_test_db();
        assert!(db.list_symbol_refs(1).unwrap().is_empty());
    }

    #[test]
    fn list_symbol_refs_returns_all() {
        let (mut db, _temp) = create_test_db();
        let fid = db.upsert_file("a.py", "h1", "python", 10, 0).unwrap();
        let syms = vec![
            make_symbol("mod.Foo", "class"),
            make_symbol("mod.bar", "function"),
        ];
        db.insert_symbols(fid, "a.py", &syms, 1, None).unwrap();

        let refs = db.list_symbol_refs(1).unwrap();
        assert_eq!(refs.len(), 2);
        assert!(refs.iter().any(|r| r.qualname == "mod.Foo"));
        assert!(refs.iter().any(|r| r.qualname == "mod.bar"));
    }

    #[test]
    fn list_symbol_refs_excludes_deleted_file() {
        let (mut db, _temp) = create_test_db();
        let fid = db.upsert_file("a.py", "h1", "python", 10, 0).unwrap();
        db.insert_symbols(fid, "a.py", &[make_symbol("mod.Foo", "class")], 1, None)
            .unwrap();
        db.mark_file_deleted("a.py", 1).unwrap();
        assert!(db.list_symbol_refs(1).unwrap().is_empty());
    }

    #[test]
    fn list_symbol_refs_wrong_version() {
        let (mut db, _temp) = create_test_db();
        let fid = db.upsert_file("a.py", "h1", "python", 10, 0).unwrap();
        db.insert_symbols(fid, "a.py", &[make_symbol("mod.Foo", "class")], 1, None)
            .unwrap();
        assert!(db.list_symbol_refs(999).unwrap().is_empty());
    }

    // ---- symbol_map_for_file ----

    #[test]
    fn symbol_map_empty_file() {
        let (db, _temp) = create_test_db();
        let fid = db.upsert_file("a.py", "h1", "python", 10, 0).unwrap();
        assert!(db.symbol_map_for_file(fid, 1).unwrap().is_empty());
    }

    #[test]
    fn symbol_map_qualname_to_id() {
        let (mut db, _temp) = create_test_db();
        let fid = db.upsert_file("a.py", "h1", "python", 10, 0).unwrap();
        let syms = vec![
            make_symbol("mod.Foo", "class"),
            make_symbol("mod.bar", "function"),
        ];
        let inserted = db.insert_symbols(fid, "a.py", &syms, 1, None).unwrap();
        let map = db.symbol_map_for_file(fid, 1).unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(map["mod.Foo"], inserted[0].id);
        assert_eq!(map["mod.bar"], inserted[1].id);
    }

    #[test]
    fn symbol_map_wrong_version() {
        let (mut db, _temp) = create_test_db();
        let fid = db.upsert_file("a.py", "h1", "python", 10, 0).unwrap();
        db.insert_symbols(fid, "a.py", &[make_symbol("mod.Foo", "class")], 1, None)
            .unwrap();
        assert!(db.symbol_map_for_file(fid, 999).unwrap().is_empty());
    }

    #[test]
    fn symbol_map_nonexistent_file_id() {
        let (db, _temp) = create_test_db();
        assert!(db.symbol_map_for_file(99999, 1).unwrap().is_empty());
    }

    // ---- repo_overview ----

    #[test]
    fn repo_overview_empty() {
        let (db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        let ov = db.repo_overview("/repo".into(), None, gv).unwrap();
        assert_eq!(ov.files, 0);
        assert_eq!(ov.symbols, 0);
        assert_eq!(ov.edges, 0);
    }

    #[test]
    fn repo_overview_counts() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(Some("abc")).unwrap();
        let fid = db.upsert_file("src/a.py", "h1", "python", 10, 0).unwrap();
        let inserted = db
            .insert_symbols(
                fid,
                "src/a.py",
                &[make_symbol("a.foo", "function")],
                gv,
                None,
            )
            .unwrap();

        let mut sym_map = HashMap::new();
        sym_map.insert("a.foo".to_string(), inserted[0].id);
        db.insert_edges(
            fid,
            &[make_edge("CALLS", "a.foo", "a.foo")],
            &sym_map,
            gv,
            None,
        )
        .unwrap();

        let ov = db.repo_overview("/repo".into(), None, gv).unwrap();
        assert_eq!(ov.files, 1);
        assert_eq!(ov.symbols, 1);
        assert_eq!(ov.edges, 1);
        assert_eq!(ov.commit_sha, Some("abc".to_string()));
    }

    #[test]
    fn repo_overview_language_filter() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        let fid_py = db.upsert_file("a.py", "h1", "python", 10, 0).unwrap();
        let fid_rs = db.upsert_file("b.rs", "h2", "rust", 20, 0).unwrap();
        db.insert_symbols(fid_py, "a.py", &[make_symbol("mod.Foo", "class")], gv, None)
            .unwrap();
        db.insert_symbols(
            fid_rs,
            "b.rs",
            &[
                make_symbol("mod.Bar", "struct"),
                make_symbol("mod.Baz", "function"),
            ],
            gv,
            None,
        )
        .unwrap();

        let langs = vec!["rust".to_string()];
        let ov = db.repo_overview("/repo".into(), Some(&langs), gv).unwrap();
        assert_eq!(ov.files, 1);
        assert_eq!(ov.symbols, 2);
    }

    // ---- digest ----

    #[test]
    fn digest_empty() {
        let (db, _temp) = create_test_db();
        let d = db.digest().unwrap();
        assert_eq!(d.files.rows, 0);
        assert_eq!(d.symbols.rows, 0);
        assert_eq!(d.edges.rows, 0);
    }

    #[test]
    fn digest_changes_after_insert() {
        let (mut db, _temp) = create_test_db();
        let d1 = db.digest().unwrap();
        let fid = db.upsert_file("a.py", "h1", "python", 10, 0).unwrap();
        db.insert_symbols(fid, "a.py", &[make_symbol("mod.Foo", "class")], 1, None)
            .unwrap();
        let d2 = db.digest().unwrap();
        assert_ne!(d1, d2);
        assert!(d2.files.rows > 0);
        assert!(d2.symbols.rows > 0);
    }

    // ---- module_summary ----

    #[test]
    fn module_summary_empty() {
        let (db, _temp) = create_test_db();
        assert!(db.module_summary(1, None, None, 1).unwrap().is_empty());
    }

    #[test]
    fn module_summary_groups_by_depth() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        let fid1 = db
            .upsert_file("src/db/mod.rs", "h1", "rust", 100, 0)
            .unwrap();
        let fid2 = db
            .upsert_file("src/db/overview.rs", "h2", "rust", 50, 0)
            .unwrap();
        let fid3 = db
            .upsert_file("src/rpc/handlers.rs", "h3", "rust", 200, 0)
            .unwrap();
        db.insert_symbols(
            fid1,
            "src/db/mod.rs",
            &[make_symbol("db.Db", "struct")],
            gv,
            None,
        )
        .unwrap();
        db.insert_symbols(
            fid2,
            "src/db/overview.rs",
            &[make_symbol("db.overview.foo", "function")],
            gv,
            None,
        )
        .unwrap();
        db.insert_symbols(
            fid3,
            "src/rpc/handlers.rs",
            &[
                make_symbol("rpc.handle", "function"),
                make_symbol("rpc.route", "function"),
            ],
            gv,
            None,
        )
        .unwrap();

        let summary = db.module_summary(2, None, None, gv).unwrap();
        let db_mod = summary.iter().find(|e| e.path == "src/db/").unwrap();
        assert_eq!(db_mod.file_count, 2);
        assert_eq!(db_mod.symbol_count, 2);

        let rpc_mod = summary.iter().find(|e| e.path == "src/rpc/").unwrap();
        assert_eq!(rpc_mod.file_count, 1);
        assert_eq!(rpc_mod.symbol_count, 2);
    }

    #[test]
    fn module_summary_sorted_desc() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        let fid1 = db.upsert_file("small/a.rs", "h1", "rust", 10, 0).unwrap();
        let fid2 = db.upsert_file("big/b.rs", "h2", "rust", 10, 0).unwrap();
        db.insert_symbols(
            fid1,
            "small/a.rs",
            &[make_symbol("x", "function")],
            gv,
            None,
        )
        .unwrap();
        db.insert_symbols(
            fid2,
            "big/b.rs",
            &[
                make_symbol("a", "function"),
                make_symbol("b", "function"),
                make_symbol("c", "function"),
            ],
            gv,
            None,
        )
        .unwrap();

        let summary = db.module_summary(1, None, None, gv).unwrap();
        assert_eq!(summary[0].path, "big/");
        assert_eq!(summary[0].symbol_count, 3);
        assert_eq!(summary[1].path, "small/");
        assert_eq!(summary[1].symbol_count, 1);
    }

    #[test]
    fn module_summary_root_files_dot() {
        let (db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        db.upsert_file("main.rs", "h1", "rust", 10, 0).unwrap();
        db.upsert_file("lib.rs", "h2", "rust", 20, 0).unwrap();

        let summary = db.module_summary(1, None, None, gv).unwrap();
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].path, ".");
        assert_eq!(summary[0].file_count, 2);
    }

    #[test]
    fn module_summary_language_filter() {
        let (db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        db.upsert_file("src/a.py", "h1", "python", 10, 0).unwrap();
        db.upsert_file("src/b.rs", "h2", "rust", 20, 0).unwrap();

        let langs = vec!["python".to_string()];
        let summary = db.module_summary(1, Some(&langs), None, gv).unwrap();
        assert_eq!(summary.len(), 1);
        assert!(summary[0].languages.contains(&"python".to_string()));
    }

    #[test]
    fn module_summary_files_without_symbols() {
        let (db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        db.upsert_file("src/readme.md", "h1", "markdown", 10, 0)
            .unwrap();

        let summary = db.module_summary(1, None, None, gv).unwrap();
        assert_eq!(summary.len(), 1);
        assert_eq!(summary[0].file_count, 1);
        assert_eq!(summary[0].symbol_count, 0);
    }

    // ---- module_edges ----

    #[test]
    fn module_edges_empty() {
        let (db, _temp) = create_test_db();
        assert!(db.module_edges(1, None, 1).unwrap().is_empty());
    }

    #[test]
    fn module_edges_skips_intra_module() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        let fid = db.upsert_file("src/a.rs", "h1", "rust", 100, 0).unwrap();
        let syms = vec![
            make_symbol("a.foo", "function"),
            make_symbol("a.bar", "function"),
        ];
        let inserted = db.insert_symbols(fid, "src/a.rs", &syms, gv, None).unwrap();

        let mut sym_map = HashMap::new();
        sym_map.insert("a.foo".to_string(), inserted[0].id);
        sym_map.insert("a.bar".to_string(), inserted[1].id);
        db.insert_edges(
            fid,
            &[make_edge("CALLS", "a.foo", "a.bar")],
            &sym_map,
            gv,
            None,
        )
        .unwrap();

        assert!(db.module_edges(1, None, gv).unwrap().is_empty());
    }

    #[test]
    fn module_edges_cross_module() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        let fid1 = db.upsert_file("src/a.rs", "h1", "rust", 100, 0).unwrap();
        let fid2 = db.upsert_file("lib/b.rs", "h2", "rust", 100, 0).unwrap();

        let ins1 = db
            .insert_symbols(
                fid1,
                "src/a.rs",
                &[make_symbol("a.caller", "function")],
                gv,
                None,
            )
            .unwrap();
        let ins2 = db
            .insert_symbols(
                fid2,
                "lib/b.rs",
                &[make_symbol("b.callee", "function")],
                gv,
                None,
            )
            .unwrap();

        let mut sym_map = HashMap::new();
        sym_map.insert("a.caller".to_string(), ins1[0].id);
        sym_map.insert("b.callee".to_string(), ins2[0].id);
        db.insert_edges(
            fid1,
            &[make_edge("CALLS", "a.caller", "b.callee")],
            &sym_map,
            gv,
            None,
        )
        .unwrap();

        let result = db.module_edges(1, None, gv).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "src/");
        assert_eq!(result[0].1, "lib/");
        assert_eq!(result[0].2, 1); // calls
        assert_eq!(result[0].3, 0); // imports
    }

    #[test]
    fn module_edges_separates_calls_and_imports() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        let fid1 = db.upsert_file("src/a.rs", "h1", "rust", 100, 0).unwrap();
        let fid2 = db.upsert_file("lib/b.rs", "h2", "rust", 100, 0).unwrap();

        let ins1 = db
            .insert_symbols(
                fid1,
                "src/a.rs",
                &[
                    make_symbol("a.caller", "function"),
                    make_symbol("a.importer", "function"),
                ],
                gv,
                None,
            )
            .unwrap();
        let ins2 = db
            .insert_symbols(
                fid2,
                "lib/b.rs",
                &[make_symbol("b.mod", "module")],
                gv,
                None,
            )
            .unwrap();

        let mut sym_map = HashMap::new();
        sym_map.insert("a.caller".to_string(), ins1[0].id);
        sym_map.insert("a.importer".to_string(), ins1[1].id);
        sym_map.insert("b.mod".to_string(), ins2[0].id);
        db.insert_edges(
            fid1,
            &[
                make_edge("CALLS", "a.caller", "b.mod"),
                make_edge("IMPORTS", "a.importer", "b.mod"),
            ],
            &sym_map,
            gv,
            None,
        )
        .unwrap();

        let result = db.module_edges(1, None, gv).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].2, 1); // 1 call
        assert_eq!(result[0].3, 1); // 1 import
    }

    #[test]
    fn module_edges_sorted_by_total_desc() {
        let (mut db, _temp) = create_test_db();
        let gv = db.create_graph_version(None).unwrap();
        let fid1 = db.upsert_file("a/x.rs", "h1", "rust", 10, 0).unwrap();
        let fid2 = db.upsert_file("b/y.rs", "h2", "rust", 10, 0).unwrap();
        let fid3 = db.upsert_file("c/z.rs", "h3", "rust", 10, 0).unwrap();

        let ins1 = db
            .insert_symbols(
                fid1,
                "a/x.rs",
                &[
                    make_symbol("a.f1", "function"),
                    make_symbol("a.f2", "function"),
                ],
                gv,
                None,
            )
            .unwrap();
        let ins2 = db
            .insert_symbols(fid2, "b/y.rs", &[make_symbol("b.g", "function")], gv, None)
            .unwrap();
        let ins3 = db
            .insert_symbols(fid3, "c/z.rs", &[make_symbol("c.h", "function")], gv, None)
            .unwrap();

        let mut sym_map = HashMap::new();
        sym_map.insert("a.f1".to_string(), ins1[0].id);
        sym_map.insert("a.f2".to_string(), ins1[1].id);
        sym_map.insert("b.g".to_string(), ins2[0].id);
        sym_map.insert("c.h".to_string(), ins3[0].id);

        // a->b: 2 calls, a->c: 1 call
        db.insert_edges(
            fid1,
            &[
                make_edge("CALLS", "a.f1", "b.g"),
                make_edge("CALLS", "a.f2", "b.g"),
                make_edge("CALLS", "a.f1", "c.h"),
            ],
            &sym_map,
            gv,
            None,
        )
        .unwrap();

        let result = db.module_edges(1, None, gv).unwrap();
        assert_eq!(result.len(), 2);
        // a/->b/ (2 calls) should be first
        assert_eq!(result[0].2 + result[0].3, 2);
        assert_eq!(result[1].2 + result[1].3, 1);
    }
}
