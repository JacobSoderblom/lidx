use super::Db;
use anyhow::Result;
use rusqlite::params;

impl Db {
    // Co-change methods for git mining intelligence

    pub fn insert_co_changes_batch(
        &mut self,
        entries: &[crate::git_mining::CoChangeEntry],
    ) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }

        let mined_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let mut count = 0;

        {
            let mut insert = tx.prepare(
                "INSERT INTO co_changes
                 (file_a, file_b, co_change_count, total_commits_a, total_commits_b, confidence, last_commit_sha, last_commit_ts, mined_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(file_a, file_b) DO UPDATE SET
                   co_change_count = excluded.co_change_count,
                   total_commits_a = excluded.total_commits_a,
                   total_commits_b = excluded.total_commits_b,
                   confidence = excluded.confidence,
                   last_commit_sha = excluded.last_commit_sha,
                   last_commit_ts = excluded.last_commit_ts,
                   mined_at = excluded.mined_at",
            )?;

            for entry in entries {
                insert.execute(params![
                    entry.file_a,
                    entry.file_b,
                    entry.co_change_count,
                    entry.total_commits_a,
                    entry.total_commits_b,
                    entry.confidence,
                    entry.last_commit_sha,
                    entry.last_commit_ts,
                    mined_at,
                ])?;
                count += 1;
            }
        }

        tx.commit()?;
        Ok(count)
    }

    pub fn co_changes_for_file(
        &self,
        path: &str,
        limit: usize,
        min_confidence: f64,
        _graph_version: i64,
    ) -> Result<Vec<crate::model::CoChangeResult>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT file_a, file_b, co_change_count, confidence, last_commit_sha
             FROM co_changes
             WHERE (file_a = ? OR file_b = ?)
               AND confidence >= ?
             ORDER BY confidence DESC
             LIMIT ?",
        )?;

        let rows = stmt.query_map(params![path, path, min_confidence, limit as i64], |row| {
            Ok(crate::model::CoChangeResult {
                file_a: row.get(0)?,
                file_b: row.get(1)?,
                co_change_count: row.get(2)?,
                confidence: row.get(3)?,
                last_commit_sha: row.get(4)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn co_changes_for_files(
        &self,
        paths: &[String],
        limit: usize,
        min_confidence: f64,
        _graph_version: i64,
    ) -> Result<Vec<crate::model::CoChangeResult>> {
        if paths.is_empty() {
            return Ok(Vec::new());
        }

        let conn = self.read_conn()?;

        // Build query with IN clause
        let placeholders = paths.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let sql = format!(
            "SELECT file_a, file_b, co_change_count, confidence, last_commit_sha
             FROM co_changes
             WHERE (file_a IN ({}) OR file_b IN ({}))
               AND confidence >= ?
             ORDER BY confidence DESC
             LIMIT ?",
            placeholders, placeholders
        );

        let mut stmt = conn.prepare(&sql)?;

        // Build params: paths, paths, min_confidence, limit
        let mut params: Vec<&dyn rusqlite::ToSql> = Vec::new();
        for path in paths {
            params.push(path);
        }
        for path in paths {
            params.push(path);
        }
        params.push(&min_confidence);
        let limit_param = limit as i64;
        params.push(&limit_param);

        let rows = stmt.query_map(&*params, |row| {
            Ok(crate::model::CoChangeResult {
                file_a: row.get(0)?,
                file_b: row.get(1)?,
                co_change_count: row.get(2)?,
                confidence: row.get(3)?,
                last_commit_sha: row.get(4)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn clear_co_changes(&mut self) -> Result<()> {
        self.conn().execute("DELETE FROM co_changes", [])?;
        Ok(())
    }

    pub fn coupling_hotspots(
        &self,
        limit: usize,
        min_confidence: f64,
    ) -> Result<Vec<crate::model::CouplingHotspot>> {
        let conn = self.read_conn()?;
        let mut stmt = conn.prepare(
            "SELECT file_a, file_b, confidence, co_change_count
             FROM co_changes
             WHERE confidence >= ?
             ORDER BY confidence DESC
             LIMIT ?",
        )?;

        let rows = stmt.query_map(params![min_confidence, limit as i64], |row| {
            Ok(crate::model::CouplingHotspot {
                file_a: row.get(0)?,
                file_b: row.get(1)?,
                confidence: row.get(2)?,
                co_change_count: row.get(3)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
}
